from enum import Enum
from typing import Awaitable
from typing import Callable
from typing import ParamSpec
from typing import TypeVar

type JSON = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None


class FlowState(str, Enum):
    WAITING = "WAITING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    FAILED_FINALLY = "FAILED_FINALLY"
    ORCHESTRATOR_LOST = "ORCHESTRATOR_LOST"
    ORPHANED = "ORPHANED"


type Hash = str

Params = ParamSpec("Params")
Result = TypeVar("Result")

type FlowFunction = Callable[Params, Awaitable[Result]]


class NoCacheHit(Exception):
    pass
