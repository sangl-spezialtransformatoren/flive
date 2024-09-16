import warnings
from contextvars import ContextVar
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import ClassVar
from typing import Generic
from typing import Optional
from uuid import UUID

from loguru import logger
from sqlalchemy.exc import NoResultFound
from uuid_utils import uuid7

from flive.orchestrator import get_current_orchestrator
from flive.serialize import SerializedParams
from flive.serialize import deserialize_parameters
from flive.serialize import deserialize_result
from flive.serialize import serialize_parameters
from flive.serialize import serialize_result
from flive.types import JSON
from flive.types import FlowFunction
from flive.types import Params
from flive.types import Result

current_flow: ContextVar[UUID | None] = ContextVar("current_flow", default=None)


def shorten_flow_id(flow_id: UUID) -> str:
    return str(flow_id)[-8:]


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
        flow_id_short = shorten_flow_id(flow_id)
        logger.debug(f"Starting execute_work for flow_id: {flow_id}", flow_id=flow_id_short)
        orchestrator = get_current_orchestrator()
        logger.debug(f"Got current orchestrator: {orchestrator}", flow_id=flow_id_short)

        # Mark, that we picked up the work
        await orchestrator.start_work_on_flow(flow_id=flow_id)
        logger.debug(f"Marked flow {flow_id} as started", flow_id=flow_id_short)

        result = None
        exception = None
        token = current_flow.set(flow_id)
        try:
            logger.debug(f"Executing function for flow {flow_id}", flow_id=flow_id_short)
            result = await self._func(*args, **kwargs)
            logger.debug(f"Function execution completed for flow {flow_id}", flow_id=flow_id_short)
        except Exception as e:
            exception = e
            logger.exception(f"Flow {flow_id} failed with exception", exc_info=exception, flow_id=flow_id_short)
        finally:
            current_flow.reset(token)

        # Get result
        if not exception:
            logger.debug(f"Flow {flow_id} completed successfully", flow_id=flow_id_short)
            # Write result and delete flow from active flows
            serialized_result = self.serialize_result(result)
            await orchestrator.complete_flow(flow_id, serialized_result)
            logger.debug(f"Marked flow {flow_id} as complete with result: {serialized_result}", flow_id=flow_id_short)
            return result

        logger.debug(f"Flow {flow_id} failed, getting failure info", flow_id=flow_id_short)
        failure_info = await orchestrator.fail_flow(flow_id)
        logger.debug(f"Failure info for flow {flow_id}: {failure_info}", flow_id=flow_id_short)

        if failure_info.retries_left:
            logger.debug(f"Retrying flow {flow_id}", flow_id=flow_id_short)
            # Retry the flow
            return await self.execute_work(flow_id, *args, **kwargs)

        elif failure_info.has_parent_flow:
            logger.debug(f"Flow {flow_id} has parent flow, raising exception", flow_id=flow_id_short)
            # If there is a parent flow, that has to handle the exception
            raise exception

    async def execute_work_json(self, flow_id: UUID, parameters: SerializedParams):
        """Executes a task with params from a SerializedParams object."""

        flow_id_short = shorten_flow_id(flow_id)
        logger.debug(f"Starting execute_work_json for flow_id: {flow_id}", flow_id=flow_id_short)
        args, kwargs = self.deserialize_parameters(parameters)
        await self.execute_work(flow_id, *args, **kwargs)
        logger.debug(f"Completed execute_work_json for flow_id: {flow_id}", flow_id=flow_id_short)

    async def __call__(
        self,
        *args: Params.args,
        **kwargs: Params.kwargs
    ) -> Result:

        # Obtain active orchestrator
        orchestrator = get_current_orchestrator()
        logger.debug(f"Obtained orchestrator: {orchestrator}")

        # Just execute the function if no orchestrator is set up.
        if not orchestrator:
            logger.warning("No orchestrator is set up. Executing function directly without flow management.")
            warnings.warn(
                "No orchestrator is set up. Executing function directly without flow management.",
                RuntimeWarning,
            )
            return await self._func(*args, **kwargs)

        # Serialize parameters
        parameters = self.serialize_parameters(*args, **kwargs)
        logger.debug(f"Serialized parameters: {parameters}")

        parent_flow_id = current_flow.get()
        logger.debug(f"Parent flow ID: {parent_flow_id}")
        flow_id = None

        if parent_flow_id:
            logger.debug("Attempting to retrieve cached result or recover orphaned flow")
            # Try to return a cached result or recover an orphaned flow
            try:
                serialized_result = await orchestrator.get_cached_result_of_flow(
                    flow_key=self.key,
                    parent_flow_id=parent_flow_id,
                    parameters=parameters,
                )
                logger.debug(f"Retrieved cached result: {serialized_result}")
                return self.deserialize_result(serialized_result)
            except NoResultFound:
                logger.debug("No cached result found")
        
            try:
                acquired_flow = await orchestrator.recover_orphaned_flow(
                    flow_key=self.key,
                    parent_flow_id=parent_flow_id,
                    parameters=parameters
                )
                flow_id = acquired_flow.id
                logger.debug(f"Recovered orphaned flow with ID: {flow_id}", flow_id=shorten_flow_id(flow_id))
            except NoResultFound:
                logger.debug("No orphaned flow found")

        if not flow_id:
            # Create a new flow if no cached result or orphaned flow was found
            flow_id = uuid7()
            logger.debug(f"Creating new flow with ID: {flow_id}", flow_id=shorten_flow_id(flow_id))
            await orchestrator.dispatch_and_acquire_flow(
                flow_id=flow_id,
                key=self.key,
                parameters=parameters,
                retries=self.retries,
                parent_flow_id=parent_flow_id,
            )

        logger.debug(f"Executing work for flow ID: {flow_id}", flow_id=shorten_flow_id(flow_id))
        return await self.execute_work(flow_id, *args, **kwargs)

    async def dispatch(
        self,
        /,
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

        await orchestrator.dispatch_flow(
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
