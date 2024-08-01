from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Literal
from typing import ParamSpec
from typing import TypeVar

type JSON = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None

type FlowState = Literal[
    "WAITING", "SCHEDULED", "RUNNING", "COMPLETED", "FAILED", "FINALLY_FAILED"
]

type FlowEvent = Literal[
    "DISPATCHED",
    "SCHEDULED",
    "STARTED",
    "COMPLETED",
    "FAILED",
    "FINALLY_FAILED",
    "RETRY",
    "ORCHESTRATOR_LOST",
]


type Hash = str

Params = ParamSpec("Params")
Result = TypeVar("Result")

type ParamsUntyped = tuple[list[Any], dict[str, Any]]
type FlowFunction = Callable[Params, Awaitable[Result]]


class NoCacheHit(Exception):
    pass
