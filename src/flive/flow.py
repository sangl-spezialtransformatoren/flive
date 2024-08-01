from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Generic
from typing import Optional
from typing import ParamSpec
from typing import TypeVar

_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")


class Flow(Generic[_FuncParams, _ReturnType]):
    _func: Callable[_FuncParams, Awaitable[_ReturnType]]
    _on_commit: Optional[Callable[[Any], Awaitable[Any]]]

    def __init__(self, func: Callable[_FuncParams, Awaitable[_ReturnType]]):
        self._func = func
        self._on_commit = None

    def on_commit(self, func: Callable[[Any], Awaitable[Any]]):
        self._on_commit = func

    def __call__(  # noqa: D102
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> Awaitable[_ReturnType]:
        return self._func(*args, **kwargs)

    def delay(  # noqa: D102
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> Awaitable[_ReturnType]:
        return self._func(*args, **kwargs)


def flow(name: str) -> Callable[
    [Callable[_FuncParams, Awaitable[_ReturnType]]], Flow[_FuncParams, _ReturnType]]:
    def inner(func: Any):
        return Flow(func)

    return inner
