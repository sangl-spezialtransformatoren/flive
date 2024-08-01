import asyncio
from contextvars import ContextVar
from contextvars import Token
from typing import TYPE_CHECKING
from typing import Optional

from flive.backends.common import get_current_backend

if TYPE_CHECKING:
    from flive.flow import Flow

_active_orchestrator: ContextVar[Optional["Orchestrator"]] = ContextVar(
    "_active_orchestrator", default=None
)


def get_active_orchestrator() -> Optional["Orchestrator"]:
    return _active_orchestrator.get()


class Orchestrator:
    flows: dict[str, "Flow"]
    token: Optional[Token]

    def __init__(self, flows: list["Flow"]) -> None:
        self.flows = {flow.key: flow for flow in flows}

    async def setup(self):
        backend = get_current_backend()
        self.token = _active_orchestrator.set(self)
        await backend.initialize_instrumentation()

    async def teardown(self):
        _active_orchestrator.reset(self.token)

    async def __aenter__(self):
        await self.setup()

    async def __aexit__(self, *_):
        await self.teardown()

    async def run(self):
        backend = get_current_backend()
        async with self:
            while True:
                key, id_, params = await backend.flow_acquire_one(self.flows.keys())
                flow = self.flows[key]
                asyncio.create_task(flow.execute_work_json(id_, params))
