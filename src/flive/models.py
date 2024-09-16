from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Literal
from typing import Optional
from uuid import UUID

from sqlalchemy import JSON as SQLAlchemyJson
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Interval
from sqlalchemy import String
from sqlalchemy import TypeDecorator
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from uuid_utils import uuid7

from flive.types import JSON
from flive.types import FlowState
from flive.types import Hash


class EpochSeconds(TypeDecorator):
    impl = Float
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.timestamp()
        raise ValueError("Expected datetime object")

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return datetime.fromtimestamp(value, tz=timezone.utc)


class EpochTimedelta(TypeDecorator):
    impl = Float
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, timedelta):
            return value.total_seconds()
        raise ValueError("Expected timedelta object")

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return timedelta(seconds=value)


class BaseModel(DeclarativeBase):
    type_annotation_map = {
        # The datetime implementation for SQLite is not able to calculate
        # the difference between datetimes/intervals, so we need to use custom types.
        datetime: DateTime(timezone=True).with_variant(EpochSeconds(), "sqlite"),
        timedelta: Interval().with_variant(EpochTimedelta(), "sqlite"),

        # Store json data in PostgreSQL as JSONB for better performance
        JSON: SQLAlchemyJson().with_variant(JSONB, "postgresql"),

        # Use a fixed length string to store the hash
        Hash: String(16),

        # Use a string to store the flow state instead of an ENUM
        FlowState: String,
    }


class FlowModel(BaseModel):
    __tablename__ = "flow"

    # General data
    id: Mapped[UUID] = mapped_column(primary_key=True)
    key: Mapped[str]
    state: Mapped[FlowState] = mapped_column(default=FlowState.WAITING)
    parameters: Mapped[JSON]
    parameter_hash: Mapped[Hash] = mapped_column(index=True)
    result: Mapped[Optional[JSON]]
    parent_flow_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("flow.id"), index=True
    )

    # Retry handling
    maximum_retries: Mapped[int] = mapped_column(default=0)
    retries: Mapped[int] = mapped_column(default=0)


class SettingsModel(BaseModel):
    __tablename__ = "settings"

    id: Mapped[int] = mapped_column(default=0, primary_key=True)
    heartbeat_interval: Mapped[timedelta]
    orchestrator_dead_after: Mapped[timedelta]


class OrchestratorModel(BaseModel):
    __tablename__ = "orchestrator"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    last_seen_at: Mapped[datetime]


class FlowAssignmentModel(BaseModel):
    __tablename__ = "flow_assignment"

    flow_id: Mapped[UUID] = mapped_column(ForeignKey("flow.id"), primary_key=True)
    orchestrator_id: Mapped[UUID] = mapped_column(ForeignKey("orchestrator.id"))

    __table_args__ = (
        UniqueConstraint("flow_id", "orchestrator_id", name="uq_flow_assignment"),
    )


class FlowHistoryModel(BaseModel):
    __tablename__ = "flow_history"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    flow_id: Mapped[UUID] = mapped_column(ForeignKey("flow.id"))
    timestamp: Mapped[datetime] = mapped_column(default=lambda: datetime.now(tz=UTC))
    from_state: Mapped[Optional[FlowState]]
    to_state: Mapped[FlowState]


class FlowLogs(BaseModel):
    __tablename__ = "flow_logs"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    kind: Mapped[Literal["stdout", "stderr"]]
    flow_id: Mapped[UUID] = mapped_column(ForeignKey("flow.id"))
    timestamp: Mapped[datetime] = mapped_column(default=lambda: datetime.now(tz=UTC))
    message: Mapped[str]
