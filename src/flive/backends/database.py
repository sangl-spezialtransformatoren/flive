import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from typing import Optional
from typing import cast
from uuid import UUID

import xxhash
from sqlalchemy import JSON as SQLAlchemyJson
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import and_
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import literal_column
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import InterfaceError
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from tenacity import RetryCallState
from tenacity import retry as tenacity_retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_delay
from tenacity import wait_random_exponential
from uuid_utils import uuid7

from flive.backends.abstract import AbstractBackend
from flive.serialize.pydantic import SerializedParams
from flive.types import JSON
from flive.types import FlowEvent
from flive.types import FlowState
from flive.types import Hash
from flive.types import NoCacheHit

# Tenacity settings
retry_on = (ConnectionError, OSError, InterfaceError)

retry = tenacity_retry(
    retry=retry_if_exception_type(retry_on),
    stop=stop_after_delay(60),
    wait=wait_random_exponential(0.1, max=3),
    reraise=True,
)


def acquire_wait(state: RetryCallState):
    try:
        raise state.outcome.exception()
    except NoResultFound:
        return 1
    except Exception:
        return wait_random_exponential(0.1, max=3)(state)


def acquire_stop(state: RetryCallState):
    try:
        raise state.outcome.exception()
    except NoResultFound:
        return False
    except Exception:
        return stop_after_delay(60)(state)


acquire_retry = tenacity_retry(
    retry=retry_if_exception_type((*retry_on, NoResultFound)),
    wait=acquire_wait,
    stop=acquire_stop,
    reraise=True,
)


# Parameter hashing
def dict_hash(params: SerializedParams) -> str:
    json_string = json.dumps(
        params, sort_keys=True, ensure_ascii=True, separators=(",", ":")
    )
    return xxhash.xxh3_64_hexdigest(json_string)


# Database models
class Base(DeclarativeBase):
    type_annotation_map = {
        datetime: DateTime(timezone=True),
        JSON: SQLAlchemyJson().with_variant(JSONB, "postgresql"),
        Hash: String(16),
    }


class Settings(Base):
    __tablename__ = "settings"

    id: Mapped[int] = mapped_column(default=0, primary_key=True)
    heartbeat_interval: Mapped[timedelta]
    orchestrator_dead_after: Mapped[timedelta]


class Orchestrator(Base):
    __tablename__ = "orchestrator"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    last_seen_at: Mapped[datetime]


class Flow(Base):
    __tablename__ = "flow"

    # General data
    id: Mapped[UUID] = mapped_column(primary_key=True)
    key: Mapped[str]
    state: Mapped[FlowState] = mapped_column(default="WAITING")
    parameters: Mapped[JSON]
    parameter_hash: Mapped[Hash] = mapped_column(index=True)
    result: Mapped[Optional[JSON]]
    parent_flow_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("flow.id"), index=True
    )

    # Retry handling
    maximum_retries: Mapped[int] = mapped_column(default=0)
    retries: Mapped[int] = mapped_column(default=0)


class FlowAssignment(Base):
    __tablename__ = "flow_assignment"

    flow_id: Mapped[UUID] = mapped_column(ForeignKey("flow.id"), primary_key=True)
    orchestrator_id: Mapped[UUID] = mapped_column(ForeignKey("orchestrator.id"))


class FlowHistory(Base):
    __tablename__ = "flow_history"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    flow_id: Mapped[UUID] = mapped_column(ForeignKey("flow.id"))
    timestamp: Mapped[datetime] = mapped_column(default=lambda: datetime.now(tz=UTC))
    event: Mapped[FlowEvent]


class FlowLogs(Base):
    __tablename__ = "flow_logs"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    kind: Mapped[Literal["stdout", "stderr"]]
    flow_id: Mapped[UUID] = mapped_column(ForeignKey("flow.id"))
    timestamp: Mapped[datetime] = mapped_column(default=lambda: datetime.now(tz=UTC))
    message: Mapped[str]


class DatabaseBackend(AbstractBackend):
    id: UUID
    session: Optional[async_sessionmaker[AsyncSession]]

    def __init__(self, connection_string: str) -> None:
        super().__init__()
        self.id = uuid7()

        engine = create_async_engine(connection_string, echo=False)
        self.session = async_sessionmaker(engine, expire_on_commit=False)

    def settings_cte(self):
        return select(Settings).limit(1).cte("settings")

    async def register(self):
        async with self.session() as session:
            await session.execute(
                insert(Orchestrator).values(
                    id=self.id, last_seen_at=datetime.now(tz=UTC)
                )
            )
            await session.commit()

    @retry
    async def heartbeat(self):
        async with self.session() as session:
            await session.execute(
                update(Orchestrator)
                .where(Orchestrator.id == self.id)
                .values(last_seen_at=datetime.now(tz=UTC))
            )
            await session.commit()

    @retry
    async def flow_acquire_lock(self, flow_id: UUID):
        async with self.session() as session:
            existing_lock = await session.get(
                FlowAssignment, flow_id, with_for_update=True
            )

            if existing_lock:
                raise RuntimeError("Flow already locked")

            association = FlowAssignment(flow_id=flow_id, orchestrator_id=self.id)
            session.add(association)
            await session.commit()

    @retry
    async def flow_release_lock(self, flow_id: UUID):
        async with self.session() as session:
            await session.execute(
                delete(FlowAssignment).where(FlowAssignment.flow_id == flow_id)
            )
            await session.commit()

    @retry
    async def flow_dispatch(
        self,
        flow_id: UUID,
        key: str,
        parameters: JSON,
        retries: int = 0,
        parent_flow_id: UUID = None,
        scheduled: bool = False,
    ):
        parameter_hash = dict_hash(parameters)
        try:
            async with self.session() as session:
                flow = Flow(
                    id=flow_id,
                    key=key,
                    state="SCHEDULED" if scheduled else "WAITING",
                    parameters=parameters,
                    parameter_hash=parameter_hash,
                    maximum_retries=retries,
                    parent_flow_id=parent_flow_id,
                )
                session.add(flow)

                event = FlowHistory(
                    flow_id=flow_id, event="SCHEDULED" if scheduled else "DISPATCHED"
                )
                session.add(event)
                await session.commit()
        except Exception as e:
            print(e)

    @retry
    async def flow_start_work(self, flow_id: UUID):
        async with self.session() as session:
            # Write event
            event = FlowHistory(flow_id=flow_id, event="STARTED")
            session.add(event)

            # Update values
            await session.execute(
                update(Flow).where(Flow.id == flow_id).values(state="RUNNING")
            )

            await session.commit()

    @retry
    async def flow_complete(self, flow_id: UUID, result: JSON):
        async with self.session() as session:
            # Mark flow as completed and write result
            flow = await session.get(Flow, flow_id, with_for_update=True)
            flow.state = "COMPLETED"
            flow.result = result

            # Add event
            event = FlowHistory(flow_id=flow_id, event="COMPLETED")
            session.add(event)

            # Release lock
            await session.execute(
                delete(FlowAssignment).where(FlowAssignment.flow_id == flow_id)
            )

            await session.commit()

    @retry
    async def flow_fail(self, flow_id: UUID, e: Exception):
        async with self.session() as session:
            flow = await session.get(Flow, flow_id, with_for_update=True)
            parent_id = flow.parent_flow_id

            # Flow failed finally, if the maximum number of retries is reached
            failed_finally = flow.retries >= flow.maximum_retries
            flow.state = "FINALLY_FAILED" if failed_finally else "FAILED"

            # Add event
            event = FlowHistory(
                flow_id=flow_id, event="FINALLY_FAILED" if failed_finally else "FAILED"
            )
            session.add(event)

            # Remove assignment
            await session.execute(
                delete(FlowAssignment).where(FlowAssignment.flow_id == flow_id)
            )

            await session.commit()

        # Raise the exception only when there is a parent task that can handle the exception.
        if failed_finally and parent_id:
            raise e

    @retry
    async def flow_get_cached_result(
        self, flow_key: Hash, parent_flow_id: UUID, parameters: SerializedParams
    ) -> JSON:
        # If the subflow has run before, we can identify it by key, parent flow id, parameter hash and state.
        parameter_hash = dict_hash(parameters)
        stmt = (
            select(Flow)
            .where(
                Flow.key == flow_key,
                Flow.parent_flow_id == parent_flow_id,
                Flow.parameter_hash == parameter_hash,
                Flow.state.in_(("COMPLETED", "FINALLY_FAILED")),
            )
            .limit(1)
        )

        # Return the result or raise a NoCacheHit exception
        async with self.session() as session:
            try:
                flow = (await session.execute(stmt)).scalars().one()
                return flow.result
            except NoResultFound:
                raise NoCacheHit()

    @acquire_retry
    async def flow_acquire_one(
        self, flow_keys: list[str]
    ) -> tuple[str, UUID, SerializedParams]:
        events = []

        # Get the next flow to work on.
        # The viable next flows must fulfill one of the following conditions:
        #   * Flow is in waiting state
        #   * Flow is running or scheduled, but the orchestrator has died (last heartbeat older than timeout or lock missing)
        #   * Flow has failed, but there are retries left
        # TODO: Maybe fetch and start multiple flows at once

        settings_cte = self.settings_cte()
        stmt = (
            select(Flow)
            .with_for_update(skip_locked=True)
            .join(settings_cte, literal_column("true"))
            .filter(
                Flow.key.in_(flow_keys),
                Flow.parent_flow_id.is_(None),
                or_(
                    Flow.state == "WAITING",
                    and_(
                        Flow.state.in_(("RUNNING", "SCHEDULED")),
                        or_(
                            ~select(FlowAssignment)
                            .where(FlowAssignment.flow_id == Flow.id)
                            .exists(),
                            select(FlowAssignment)
                            .join(
                                Orchestrator,
                                FlowAssignment.orchestrator_id == Orchestrator.id,
                            )
                            .where(
                                FlowAssignment.flow_id == Flow.id,
                                Orchestrator.last_seen_at
                                < func.now() - settings_cte.c.orchestrator_dead_after,
                            )
                            .exists(),
                        ),
                    ),
                    and_(
                        Flow.state == "FAILED",
                        Flow.retries < Flow.maximum_retries,
                    ),
                ),
            )
            .limit(1)
        )

        async with self.session() as session:
            flow = (await session.execute(stmt)).scalar()

            if not flow:
                raise NoResultFound()

            # If the flow is running or scheduled, the original operator has died
            if flow.state in ("RUNNING", "SCHEDULED"):
                events.append(FlowHistory(flow_id=flow.id, event="ORCHESTRATOR_LOST"))
                await session.execute(
                    delete(FlowAssignment).where(FlowAssignment.flow_id == flow.id)
                )
                await session.flush()

            # If the flow has failed before, we retry
            if flow.state == "FAILED":
                events.append(FlowHistory(flow_id=flow.id, event="RETRY"))
                flow.retries += 1

            # Mark the flow as scheduled
            flow.state = "SCHEDULED"
            events.append(FlowHistory(flow_id=flow.id, event="SCHEDULED"))

            # Log events
            for event in events:
                session.add(event)

            # Lock flow
            assignment = FlowAssignment(flow_id=flow.id, orchestrator_id=self.id)
            session.add(assignment)

            await session.commit()

        return (
            flow.key,
            flow.id,
            cast(SerializedParams, flow.parameters),
        )

    @retry
    async def write_flow_logs(
        self, items: tuple[Literal["stdout", "stderr"], UUID, datetime, str]
    ):
        async with self.session() as session:
            await session.execute(
                insert(FlowLogs),
                [
                    {
                        "id": uuid7(),
                        "kind": kind,
                        "flow_id": flow_id,
                        "timestamp": timestamp,
                        "message": message,
                    }
                    for kind, flow_id, timestamp, message in items
                ],
            )
            await session.commit()

    @retry
    async def cleanup(self):
        async with self.session() as session:
            settings_cte = self.settings_cte()

            await session.execute(
                delete(Orchestrator).where(
                    Orchestrator.last_seen_at
                    < func.now() - settings_cte.c.orchestrator_dead_after,
                    ~select(FlowAssignment)
                    .where(FlowAssignment.orchestrator_id == Orchestrator.id)
                    .exists(),
                )
            )
            await session.commit()
