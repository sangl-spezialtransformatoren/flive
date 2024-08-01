from contextvars import ContextVar
from typing import TYPE_CHECKING
from typing import Optional

if TYPE_CHECKING:
    from flive.backends.abstract import AbstractBackend


_active_backend: ContextVar[Optional["AbstractBackend"]] = ContextVar(
    "_active_backend", default=None
)


def set_backend(backend: "AbstractBackend"):
    _active_backend.set(backend)


def get_current_backend() -> "AbstractBackend":
    return _active_backend.get()
