import asyncio
import logging
import sys
from contextvars import ContextVar
from contextvars import Token
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import TYPE_CHECKING
from typing import Literal
from typing import Optional
from typing import TextIO
from typing import cast
from uuid import UUID

from sqlalchemy import delete
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.exc import InterfaceError
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from tenacity import RetryCallState
from tenacity import retry as _tenacity_retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_delay
from tenacity import wait_random_exponential
from uuid_utils import uuid7

from flive.logging import FliveStreamer
from flive.logging import MultiOutputStream
from flive.models import FlowAssignmentModel
from flive.models import FlowHistoryModel
from flive.models import FlowLogs
from flive.models import FlowModel
from flive.models import OrchestratorModel
from flive.models import SettingsModel
from flive.serialize import SerializedParams
from flive.types import JSON
from flive.types import FlowState
from flive.utils import dict_hash

if TYPE_CHECKING:
    from flive.flow import Flow


# Context variable to store the active orchestrator
_active_orchestrator: ContextVar[Optional["FliveOrchestrator"]] = ContextVar(
    "_active_orchestrator", default=None
)


def set_orchestrator(orchestrator: "FliveOrchestrator"):
    _active_orchestrator.set(orchestrator)


def get_current_orchestrator() -> "FliveOrchestrator":
    return _active_orchestrator.get()


# Tenacity settings
tenacity_retry_on = (ConnectionError, OSError, InterfaceError)

tenacity_retry = _tenacity_retry(
    retry=retry_if_exception_type(tenacity_retry_on),
    stop=stop_after_delay(60),
    wait=wait_random_exponential(0.1, max=3),
    reraise=True,
)


def tenacity_acquire_wait(state: RetryCallState):
    try:
        raise state.outcome.exception()
    except NoResultFound:
        return 1
    except Exception:
        return wait_random_exponential(0.1, max=3)(state)


def tenacity_acquire_stop(state: RetryCallState):
    try:
        raise state.outcome.exception()
    except NoResultFound:
        return False
    except Exception:
        return stop_after_delay(60)(state)


tenacity_acquire_retry = _tenacity_retry(
    retry=retry_if_exception_type((*tenacity_retry_on, NoResultFound)),
    wait=tenacity_acquire_wait,
    stop=tenacity_acquire_stop,
    reraise=True,
)


# Return types
@dataclass
class FlowFailureInfo:
    retries_left: bool
    has_parent_flow: bool


@dataclass
class AcquiredFlow:
    id: UUID
    key: str
    parameters: SerializedParams


# Orchestrator implementation
class FliveOrchestrator(object):
    id: UUID
    log_streamer: FliveStreamer
    stream_task: Optional[asyncio.Task]
    heartbeat_task: Optional[asyncio.Task]
    _reset_token: Optional[Token]
    _original_stdout: TextIO
    _original_stderr: TextIO
    _cached_settings: Optional[SettingsModel]
    session: Optional[async_sessionmaker[AsyncSession]]
    flows: dict[str, "Flow"]

    def __init__(
        self, connection_string: str, flows: Optional[list["Flow"]] = None
    ) -> None:
        super().__init__()
        self.id = uuid7()  # We use UUID7 to have a unique ID that is also sortable
        self.log_streamer = FliveStreamer()
        self._cached_settings = None
        self._reset_token = None
        if flows is None:
            from flive.flow import Flow

            self.flows = Flow._flows
        else:
            self.flows = {flow.key: flow for flow in flows}

        engine = create_async_engine(connection_string, echo=False)
        self.session = async_sessionmaker(engine, expire_on_commit=False)

    async def __aenter__(self):
        self._reset_token = _active_orchestrator.set(self)
        await self.setup()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        _active_orchestrator.reset(self._reset_token)
        await self.teardown()

    async def setup(self):
        await self.initialize_instrumentation()

    async def teardown(self):
        if self.stream_task:
            self.stream_task.cancel()
        if self.heartbeat_task:
            self.heartbeat_task.cancel()

    async def run(self):
        async with self:
            maintenance_task = asyncio.create_task(self._run_maintenance())
            cleanup_task = asyncio.create_task(self._run_cleanup())
            flow_task = asyncio.create_task(self._run_flows())
            try:
                await asyncio.gather(maintenance_task, cleanup_task, flow_task)
            except asyncio.CancelledError:
                pass
            finally:
                maintenance_task.cancel()
                cleanup_task.cancel()
                flow_task.cancel()
                await asyncio.gather(
                    maintenance_task, cleanup_task, flow_task, return_exceptions=True
                )

    async def _run_flows(self):
        while True:
            try:
                acquired_flow = await self.acquire_next_flow(list(self.flows.keys()))
                flow = self.flows[acquired_flow.key]
                asyncio.create_task(
                    flow.execute_work_json(acquired_flow.id, acquired_flow.parameters)
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error during flow execution: {e}")
                await asyncio.sleep(1)

    async def _run_maintenance(self):
        while True:
            try:
                await self.maintenance()
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error during maintenance: {e}")
                await asyncio.sleep(1)

    async def _run_cleanup(self):
        while True:
            try:
                await self.cleanup()
                await asyncio.sleep(60)  # Run cleanup every 60 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error during cleanup: {e}")
                await asyncio.sleep(1)

    async def initialize_instrumentation(self):
        # TODO: Simplify setup/initialization

        await self.register()
        await self.cleanup()

        # Redirect stdout and stderr into database and original stdout and stderr
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr

        sys.stdout = MultiOutputStream(self._original_stdout, self.log_streamer.stdout)
        sys.stderr = MultiOutputStream(self._original_stderr, self.log_streamer.stderr)

        # Add flive stderr to the root logger
        logger = logging.getLogger()
        logger.addHandler(logging.StreamHandler(self.log_streamer.stderr))

        # Stream logs to the database
        self.stream_task = asyncio.create_task(self.log_streamer.work())

        # Get heartbeat period from database
        settings = await self.get_settings()
        heartbeat_period = settings.heartbeat_interval.total_seconds()

        async def heartbeat():
            while True:
                await asyncio.sleep(heartbeat_period)
                await self.heartbeat()

        self.heartbeat_task = asyncio.create_task(heartbeat())

    @tenacity_retry
    async def get_settings(self) -> SettingsModel:
        """
        Retrieve the settings from the database.
        """
        async with self.session() as session:
            try:
                settings = await session.execute(select(SettingsModel).limit(1))
                result = settings.scalars().one()
            except NoResultFound:
                # If no settings are found, create default settings
                result = SettingsModel()
                session.add(result)
                await session.commit()

        self._cached_settings = result
        return result

    async def register(self) -> None:
        # Before we can use the orchestrator, we need toregister it so we can identify by the heartbeat (`last_seen_at`)
        # if the orchestrator has died
        async with self.session() as session:
            await session.execute(
                insert(OrchestratorModel).values(
                    id=self.id, last_seen_at=datetime.now(tz=UTC)
                )
            )
            await session.commit()

    @tenacity_retry
    async def heartbeat(self) -> None:
        # As long as the orchestrator is alive, we update the last_seen_at timestamp. When it dies, we can
        # identify it by the timestamp and let other orchestrators take over the assigned flows.

        # TODO: Commit suicide if heartbeat fails

        async with self.session() as session:
            await session.execute(
                update(OrchestratorModel)
                .where(OrchestratorModel.id == self.id)
                .values(last_seen_at=datetime.now(tz=UTC))
            )
            await session.commit()

    @tenacity_retry
    async def dispatch_flow(
        self,
        flow_id: UUID,
        key: str,
        parameters: JSON,
        retries: int = 0,
        parent_flow_id: UUID | None = None,
    ) -> None:
        """
        Dispatch a flow.
        """
        async with self.session() as session:
            parameter_hash = dict_hash(parameters)
            flow = FlowModel(
                id=flow_id,
                key=key,
                state=FlowState.WAITING,
                parameters=parameters,
                parameter_hash=parameter_hash,
                maximum_retries=retries,
                parent_flow_id=parent_flow_id,
            )
            session.add(flow)

            event = FlowHistoryModel(
                flow_id=flow_id, from_state=None, to_state=FlowState.WAITING
            )
            session.add(event)

            await session.commit()

    @tenacity_retry
    async def dispatch_and_acquire_flow(
        self,
        flow_id: UUID,
        key: str,
        parameters: JSON,
        retries: int = 0,
        parent_flow_id: UUID | None = None,
    ) -> None:
        """
        Dispatch a flow and acquire it in the same transaction.
        """
        async with self.session() as session:
            parameter_hash = dict_hash(parameters)
            flow = FlowModel(
                id=flow_id,
                key=key,
                state=FlowState.SCHEDULED,
                parameters=parameters,
                parameter_hash=parameter_hash,
                maximum_retries=retries,
                parent_flow_id=parent_flow_id,
            )
            session.add(flow)

            event = FlowHistoryModel(
                flow_id=flow_id, from_state=None, to_state=FlowState.SCHEDULED
            )
            session.add(event)

            association = FlowAssignmentModel(flow_id=flow_id, orchestrator_id=self.id)
            session.add(association)
            await session.commit()

    @tenacity_retry
    async def start_work_on_flow(self, flow_id: UUID) -> None:
        """
        Start working on a flow.
        """
        async with self.session() as session:
            # Verify the flow is in the correct state and associated with this orchestrator
            flow = (
                await session.execute(
                    select(FlowModel)
                    .join(FlowAssignmentModel)
                    .where(FlowModel.id == flow_id)
                    .where(FlowAssignmentModel.orchestrator_id == self.id)
                    .with_for_update()
                )
            ).scalar_one_or_none()

            if not flow:
                raise ValueError(
                    f"Flow {flow_id} not found or not associated with this orchestrator"
                )

            if flow.state != FlowState.SCHEDULED:
                raise ValueError(
                    f"Flow {flow_id} is not in SCHEDULED state. Current state: {flow.state}"
                )

            # Add a state transition event to flow history
            event = FlowHistoryModel(
                flow_id=flow_id,
                from_state=FlowState.SCHEDULED,
                to_state=FlowState.RUNNING,
            )
            session.add(event)

            # Update the flow state to "RUNNING"
            flow.state = FlowState.RUNNING

            await session.commit()

    @tenacity_retry
    async def complete_flow(self, flow_id: UUID, result: JSON) -> None:
        """
        Complete a flow.
        """
        async with self.session() as session:
            # Verify the flow is in the correct state and associated with this orchestrator
            flow = (
                await session.execute(
                    select(FlowModel)
                    .join(FlowAssignmentModel)
                    .where(FlowModel.id == flow_id)
                    .where(FlowAssignmentModel.orchestrator_id == self.id)
                    .with_for_update()
                )
            ).scalar_one_or_none()

            if not flow:
                raise ValueError(
                    f"Flow {flow_id} not found or not associated with this orchestrator"
                )

            if flow.state != FlowState.RUNNING:
                raise ValueError(
                    f"Flow {flow_id} is not in RUNNING state. Current state: {flow.state}"
                )

            # Mark flow as completed and write result
            flow.state = FlowState.COMPLETED
            flow.result = result

            # Add a "COMPLETED" event to flow history
            event = FlowHistoryModel(
                flow_id=flow_id,
                from_state=FlowState.RUNNING,
                to_state=FlowState.COMPLETED,
            )
            session.add(event)

            # Release the flow assignment (lock)
            await session.execute(
                delete(FlowAssignmentModel).where(
                    FlowAssignmentModel.flow_id == flow_id
                )
            )

            await session.commit()

    @tenacity_retry
    async def fail_flow(self, flow_id: UUID) -> FlowFailureInfo:
        """
        Fail a flow.
        """
        async with self.session() as session:
            flow = (
                await session.execute(
                    select(FlowModel)
                    .join(FlowAssignmentModel)
                    .where(FlowModel.id == flow_id)
                    .where(FlowAssignmentModel.orchestrator_id == self.id)
                    .with_for_update()
                )
            ).scalar_one_or_none()

            if not flow:
                raise ValueError(
                    f"Flow {flow_id} not found or not associated with this orchestrator"
                )

            if flow.state != FlowState.RUNNING:
                raise ValueError(
                    f"Flow {flow_id} is not in RUNNING state. Current state: {flow.state}"
                )

            flow.state = FlowState.FAILED
            flow.retries += 1

            event = FlowHistoryModel(
                flow_id=flow_id, from_state=FlowState.RUNNING, to_state=FlowState.FAILED
            )
            session.add(event)

            parent_id = flow.parent_flow_id

            if flow.retries < flow.maximum_retries:
                flow.state = FlowState.SCHEDULED
                event = FlowHistoryModel(
                    flow_id=flow_id,
                    from_state=FlowState.FAILED,
                    to_state=FlowState.SCHEDULED,
                )
                # Keep the flow associated with this orchestrator
            else:
                flow.state = FlowState.FAILED_FINALLY
                event = FlowHistoryModel(
                    flow_id=flow_id,
                    from_state=FlowState.FAILED,
                    to_state=FlowState.FAILED_FINALLY,
                )
                # Remove the flow assignment (lock) only if it's finally failed
                await session.execute(
                    delete(FlowAssignmentModel).where(
                        FlowAssignmentModel.flow_id == flow_id
                    )
                )

            session.add(event)
            await session.commit()

        # Populate the dataclass with the relevant information
        failure_info = FlowFailureInfo(
            retries_left=flow.retries < flow.maximum_retries,
            has_parent_flow=parent_id is not None,
        )

        # Return the dataclass
        return failure_info
    
    @tenacity_retry
    async def acquire_flow(self, flow_id: UUID) -> None:
        """
        Acquire a flow.
        """
        async with self.session() as session:
            flow = await session.get(FlowModel, flow_id, with_for_update=True)

            if not flow or flow.state != FlowState.WAITING:
                raise RuntimeError("Flow not found or not in WAITING state")

            # Update flow state to SCHEDULED
            flow.state = FlowState.SCHEDULED
            event = FlowHistoryModel(
                flow_id=flow_id,
                from_state=FlowState.WAITING,
                to_state=FlowState.SCHEDULED,
            )
            session.add(event)

            # Create a new flow assignment (lock)
            assignment = FlowAssignmentModel(flow_id=flow_id, orchestrator_id=self.id)
            session.add(assignment)

            await session.commit()

    @tenacity_acquire_retry
    async def acquire_next_flow(self, flow_keys: list[str]) -> AcquiredFlow:
        """
        Acquire the next flow to work on.
        """
        # Get the next flow to work on.
        stmt = (
            select(FlowModel)
            .filter(
                FlowModel.key.in_(flow_keys),
                FlowModel.parent_flow_id.is_(None),
                FlowModel.state == FlowState.WAITING,
            )
            .limit(1)
        )

        async with self.session() as session:
            flow = (await session.execute(stmt)).scalar()

            if not flow:
                raise NoResultFound()

            # Use the flow_acquire method to mark the flow as scheduled and create assignment
            await self.acquire_flow(flow.id)

        return AcquiredFlow(
            key=flow.key,
            id=flow.id,
            parameters=cast(SerializedParams, flow.parameters),
        )

    @tenacity_retry
    async def get_cached_result_of_flow(
        self, flow_key: str, parent_flow_id: UUID, parameters: SerializedParams
    ) -> JSON:
        """
        Get the cached result of a flow.
        """
        # If the subflow has run before, we can identify it by key, parent flow id, parameter hash and state
        parameter_hash = dict_hash(parameters)
        stmt = (
            select(FlowModel)
            .where(
                FlowModel.key == flow_key,
                FlowModel.parent_flow_id == parent_flow_id,
                FlowModel.parameter_hash == parameter_hash,
                FlowModel.state.in_(("COMPLETED", "FINALLY_FAILED")),
            )
            .limit(1)
        )

        # Return the result or raise a NoCacheHit exception
        async with self.session() as session:
            flow = (await session.execute(stmt)).scalars().one()
            return flow.result

    @tenacity_retry
    async def recover_orphaned_flow(
        self, flow_key: str, parent_flow_id: UUID, parameters: SerializedParams
    ) -> AcquiredFlow:
        """
        Recover an orphaned child flow.
        """
        parameter_hash = dict_hash(parameters)
        stmt = (
            select(FlowModel)
            .where(
                FlowModel.key == flow_key,
                FlowModel.parent_flow_id == parent_flow_id,
                FlowModel.parameter_hash == parameter_hash,
                FlowModel.state == FlowState.ORPHANED,
            )
            .limit(1)
        )

        async with self.session() as session:
            flow = (await session.execute(stmt)).scalars().one()

            # Lock the flow by updating its state to SCHEDULED
            flow.state = FlowState.SCHEDULED
            event = FlowHistoryModel(
                flow_id=flow.id,
                from_state=FlowState.ORPHANED,
                to_state=FlowState.SCHEDULED,
            )
            session.add(event)

            # Create a new flow assignment
            assignment = FlowAssignmentModel(
                flow_id=flow.id,
                orchestrator_id=self.id,
            )
            session.add(assignment)

            await session.commit()

            return AcquiredFlow(
                key=flow.key,
                id=flow.id,
                parameters=cast(SerializedParams, flow.parameters),
            )

    @tenacity_retry
    async def maintenance(self) -> None:
        """
        Maintenance task that runs on the orchestrator.
        """
        async with self.session() as session:
            settings = await self.get_settings()

            # Find flows with lost orchestrators
            lost_flows_query = (
                select(FlowModel)
                .join(FlowAssignmentModel, FlowAssignmentModel.flow_id == FlowModel.id)
                .join(
                    OrchestratorModel,
                    FlowAssignmentModel.orchestrator_id == OrchestratorModel.id,
                )
                .where(
                    FlowModel.state.in_((FlowState.RUNNING, FlowState.SCHEDULED)),
                    OrchestratorModel.last_seen_at
                    < datetime.now(UTC) - settings.orchestrator_dead_after,
                )
            )
            lost_flows = (await session.execute(lost_flows_query)).scalars().all()

            # Update lost flows to ORCHESTRATOR_LOST state
            for flow in lost_flows:
                old_state = flow.state
                flow.state = FlowState.ORCHESTRATOR_LOST
                event = FlowHistoryModel(
                    flow_id=flow.id,
                    from_state=old_state,
                    to_state=FlowState.ORCHESTRATOR_LOST,
                )
                session.add(event)

                # Remove the flow assignment
                await session.execute(
                    delete(FlowAssignmentModel).where(
                        FlowAssignmentModel.flow_id == flow.id
                    )
                )

                # Transition to ORPHANED or reschedule
                if flow.parent_flow_id is not None:
                    flow.state = FlowState.ORPHANED
                    orphaned_event = FlowHistoryModel(
                        flow_id=flow.id,
                        from_state=FlowState.ORCHESTRATOR_LOST,
                        to_state=FlowState.ORPHANED,
                    )
                    session.add(orphaned_event)
                else:
                    flow.state = FlowState.WAITING
                    reschedule_event = FlowHistoryModel(
                        flow_id=flow.id,
                        from_state=FlowState.ORCHESTRATOR_LOST,
                        to_state=FlowState.WAITING,
                    )
                    session.add(reschedule_event)

            await session.commit()

    @tenacity_retry
    async def write_flow_logs(
        self, items: tuple[Literal["stdout", "stderr"], UUID, datetime, str]
    ) -> None:
        """
        Write flow logs to the database.
        """
        async with self.session() as session:
            # Bulk insert flow logs
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

    @tenacity_retry
    async def cleanup(self) -> None:
        """
        Cleanup task that runs on the orchestrator.
        """
        async with self.session() as session:
            settings = await self.get_settings()
            # Get ids of orchestrators that haven't been seen recently and have no assigned flows
            orchestrator_ids_to_delete = await session.execute(
                select(OrchestratorModel.id)
                .select_from(OrchestratorModel)
                .where(
                    OrchestratorModel.last_seen_at
                    < datetime.now(UTC) - settings.orchestrator_dead_after,
                    ~select(FlowAssignmentModel)
                    .where(FlowAssignmentModel.orchestrator_id == OrchestratorModel.id)
                    .exists(),
                )
            )
            orchestrator_ids_to_delete = orchestrator_ids_to_delete.scalars().all()

            # Remove orchestrators using the collected ids
            if orchestrator_ids_to_delete:
                await session.execute(
                    delete(OrchestratorModel)
                    .where(OrchestratorModel.id.in_(orchestrator_ids_to_delete))
                    .execution_options(synchronize_session=False)
                )
            await session.commit()
