import asyncio
import logging
from contextlib import AsyncExitStack
from contextvars import ContextVar
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Generic
from typing import Optional
from uuid import UUID

from uuid_utils import uuid7

from flive.backends.common import get_current_backend
from flive.orchestrator import Orchestrator
from flive.orchestrator import get_active_orchestrator
from flive.serialize.pydantic import SerializedParams
from flive.serialize.pydantic import pydantic_deserialize_parameters
from flive.serialize.pydantic import pydantic_deserialize_result
from flive.serialize.pydantic import pydantic_serialize_parameters
from flive.serialize.pydantic import pydantic_serialize_result
from flive.types import JSON
from flive.types import FlowFunction
from flive.types import NoCacheHit
from flive.types import Params
from flive.types import Result

current_flow: ContextVar[UUID | None] = ContextVar("current_flow", default=None)


class Flow(Generic[Params, Result]):
    key: str
    _func: FlowFunction

    def __init__(
        self, func: Callable[Params, Awaitable[Result]], key: str, retries: int
    ):
        self._func = func
        self.key = key
        self.retries = retries

    def serialize_parameters(
        self, *args: Params.args, **kwargs: Params.kwargs
    ) -> SerializedParams:
        return pydantic_serialize_parameters(self._func, *args, **kwargs)

    def deserialize_parameters(
        self, serialized_parameters: SerializedParams
    ) -> tuple[list[Any], dict[str, Any]]:  # Correct typing not possible yet
        return pydantic_deserialize_parameters(self._func, serialized_parameters)

    def serialize_result(self, value: Result) -> JSON:
        return pydantic_serialize_result(self._func, value)

    def deserialize_result(self, value: JSON) -> Result:
        return pydantic_deserialize_result(self._func, value)

    async def execute_work(
        self, flow_id: UUID, *args: Params.args, **kwargs: Params.kwargs
    ) -> Result:
        backend = get_current_backend()

        # Mark, that we picked up the work
        await backend.flow_start_work(flow_id=flow_id)

        result = None
        exception = None
        token = current_flow.set(flow_id)
        try:
            result = await self._func(*args, **kwargs)
        except Exception as e:
            exception = e
            logging.exception("Flow failed with exception", exc_info=exception)
            await asyncio.sleep(2)
        finally:
            current_flow.reset(token)

        # Get result
        if not exception:
            # Write result and delete flow from active flows
            await backend.flow_complete(flow_id, self.serialize_result(result))
            return result

        await backend.flow_fail(flow_id, exception)

    async def execute_work_json(self, flow_id: UUID, parameters: SerializedParams):
        """Executes a task with params from a SerializedParams object."""
        args, kwargs = self.deserialize_parameters(parameters)
        await self.execute_work(flow_id, *args, **kwargs)

    async def __call__(
        self,
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> Result:
        # Obtain active backend
        backend = get_current_backend()

        # Just exectue the function if no backend is set up.
        if not backend:
            return await self._func(*args, **kwargs)

        # Serialize parameters
        parameters = self.serialize_parameters(*args, **kwargs)

        # Try to return a cached result from the backend
        if (parent_flow_id := current_flow.get()) is not None:
            try:
                serialized_result = await backend.flow_get_cached_result(
                    flow_key=self.key,
                    parent_flow_id=parent_flow_id,
                    parameters=parameters,
                )
                return self.deserialize_result(serialized_result)
            except NoCacheHit:
                pass

        # TODO: Mark running instances of the same flow with same parent and parameters
        # with "operator died" and transfer the retries of them to the new flow

        # If there is no cached result, create a new flow
        # Create a unique id
        flow_id = uuid7()

        async with AsyncExitStack() as stack:
            # If there's no running orchestrator, create one on the fly and enter its context.
            orchestrator = get_active_orchestrator()
            if not orchestrator:
                orchestrator = Orchestrator(flows=[self])
                await stack.enter_async_context(orchestrator)

            # Dispatch the flow to the backend
            await backend.flow_dispatch(
                flow_id=flow_id,
                key=self.key,
                parameters=parameters,
                scheduled=True,
                retries=self.retries,
                parent_flow_id=current_flow.get(),
            )

            # Acquire lock so no other worker can start the flow
            await backend.flow_acquire_lock(flow_id)
            return await self.execute_work(flow_id, *args, **kwargs)

    async def delay(
        self,
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> UUID:
        # Create a unique id
        flow_id = uuid7()

        # Obtain active backend
        backend = get_current_backend()

        # If there is no backend, we can't schedule the flow
        if not backend:
            raise RuntimeError("No backend is set up.")

        parameters = self.serialize_parameters(*args, **kwargs)

        await backend.flow_dispatch(
            flow_id=flow_id,
            key=self.key,
            parameters=parameters,
            retries=self.retries,
            parent_flow_id=current_flow.get(),
        )

        return flow_id


def flow(
    key: Optional[str] = None, retries: int = 0
) -> Callable[[Callable[Params, Awaitable[Result]]], Flow[Params, Result]]:
    def inner(func: Any):
        nonlocal key
        if not key:
            key = func.__name__

        return Flow(func, key=key, retries=retries)

    return inner
