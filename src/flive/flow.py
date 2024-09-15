import logging
import warnings
from contextvars import ContextVar
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import ClassVar
from typing import Generic
from typing import Optional
from uuid import UUID

from uuid_utils import uuid7

from flive.orchestrator import get_current_orchestrator
from flive.serialize import SerializedParams
from flive.serialize import deserialize_parameters
from flive.serialize import deserialize_result
from flive.serialize import serialize_parameters
from flive.serialize import serialize_result
from flive.types import JSON
from flive.types import FlowFunction
from flive.types import NoCacheHit
from flive.types import Params
from flive.types import Result

current_flow: ContextVar[UUID | None] = ContextVar("current_flow", default=None)


class Flow(Generic[Params, Result]):
    key: str
    _func: FlowFunction
    _flows: ClassVar[dict[str, "Flow"]] = {}

    def __init__(
        self, func: Callable[Params, Awaitable[Result]], key: str, retries: int
    ):
        if key in self._flows:
            raise ValueError(f"Flow key '{key}' is already in use")
        self._func = func
        self.key = key
        self.retries = retries

        # Register the flow
        Flow._flows[key] = self

    def serialize_parameters(
        self, *args: Params.args, **kwargs: Params.kwargs
    ) -> SerializedParams:
        return serialize_parameters(self._func, *args, **kwargs)

    def deserialize_parameters(
        self, serialized_parameters: SerializedParams
    ) -> tuple[list[Any], dict[str, Any]]:  # Correct typing not possible yet
        return deserialize_parameters(self._func, serialized_parameters)

    def serialize_result(self, value: Result) -> JSON:
        return serialize_result(self._func, value)

    def deserialize_result(self, value: JSON) -> Result:
        return deserialize_result(self._func, value)

    async def execute_work(
        self, flow_id: UUID, *args: Params.args, **kwargs: Params.kwargs
    ) -> Result:
        orchestrator = get_current_orchestrator()

        # Mark, that we picked up the work
        await orchestrator.flow_start_work(flow_id=flow_id)

        result = None
        exception = None
        token = current_flow.set(flow_id)
        try:
            result = await self._func(*args, **kwargs)
        except Exception as e:
            exception = e
            logging.exception("Flow failed with exception", exc_info=exception)
        finally:
            current_flow.reset(token)

        # Get result
        if not exception:
            # Write result and delete flow from active flows
            await orchestrator.flow_complete(flow_id, self.serialize_result(result))
            return result

        failure_info = await orchestrator.flow_fail(flow_id)

        if failure_info.retries_left:
            # Retry the flow
            return await self.execute_work(flow_id, *args, **kwargs)

        elif failure_info.has_parent_flow:
            # If there is a parent flow, that has to handle the exception
            raise exception

    async def execute_work_json(self, flow_id: UUID, parameters: SerializedParams):
        """Executes a task with params from a SerializedParams object."""
        args, kwargs = self.deserialize_parameters(parameters)
        await self.execute_work(flow_id, *args, **kwargs)

    async def __call__(
        self,
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> Result:
        # Obtain active orchestrator
        orchestrator = get_current_orchestrator()

        # Just execute the function if no orchestrator is set up.
        if not orchestrator:
            warnings.warn(
                "No orchestrator is set up. Executing function directly without flow management.",
                RuntimeWarning,
            )
            return await self._func(*args, **kwargs)

        # Serialize parameters
        parameters = self.serialize_parameters(*args, **kwargs)

        # Try to return a cached result from the orchestrator
        if (parent_flow_id := current_flow.get()) is not None:
            try:
                serialized_result = await orchestrator.flow_get_cached_result(
                    flow_key=self.key,
                    parent_flow_id=parent_flow_id,
                    parameters=parameters,
                )
                return self.deserialize_result(serialized_result)
            except NoCacheHit:
                pass

        # If there is no cached result, create a new flow
        # Create a unique id
        flow_id = uuid7()

        # Dispatch and acquire the flow in the orchestrator
        await orchestrator.flow_dispatch_and_acquire(
            flow_id=flow_id,
            key=self.key,
            parameters=parameters,
            retries=self.retries,
            parent_flow_id=current_flow.get(),
        )

        return await self.execute_work(flow_id, *args, **kwargs)

    async def dispatch(
        self,
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> UUID:
        # Create a unique id
        flow_id = uuid7()

        # Obtain active orchestrator
        orchestrator = get_current_orchestrator()

        # If there is no orchestrator, we can't schedule the flow
        if not orchestrator:
            raise RuntimeError("No orchestrator is set up.")

        parameters = self.serialize_parameters(*args, **kwargs)

        await orchestrator.flow_dispatch(
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
