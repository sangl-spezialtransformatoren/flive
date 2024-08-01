import abc
import asyncio
import logging
import sys
from datetime import datetime
from typing import Literal
from typing import Optional
from typing import TextIO
from uuid import UUID

from flive.backends.common import set_backend
from flive.logging import FliveStreamer
from flive.logging import MultiOutputStream
from flive.serialize.pydantic import SerializedParams
from flive.types import JSON

HEARTBEAT_PERIOD = 1


class AbstractBackend(abc.ABC):
    log_streamer: FliveStreamer
    stream_task: Optional[asyncio.Task]
    heartbeat_task: Optional[asyncio.Task]
    _original_stdout: TextIO
    _original_stderr: TextIO

    def __init__(self) -> None:
        super().__init__()
        self.log_streamer = FliveStreamer()

    def activate(self):
        set_backend(self)

    async def initialize_instrumentation(self):
        await self.register()
        await self.cleanup()

        # Redirect stdout and stderr into backend and original stdout and stderr
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr

        sys.stdout = MultiOutputStream(self._original_stdout, self.log_streamer.stdout)
        sys.stderr = MultiOutputStream(self._original_stderr, self.log_streamer.stderr)

        # Add flive stderr to the root logger
        logger = logging.getLogger()
        logger.addHandler(logging.StreamHandler(self.log_streamer.stderr))

        # Stream logs to backend
        self.stream_task = asyncio.create_task(self.log_streamer.work())

        async def heartbeat():
            while True:
                await asyncio.sleep(HEARTBEAT_PERIOD)
                await self.heartbeat()

        self.heartbeat_task = asyncio.create_task(heartbeat())

    @abc.abstractmethod
    async def register(self): ...

    @abc.abstractmethod
    async def heartbeat(self): ...

    @abc.abstractmethod
    async def cleanup(self): ...

    @abc.abstractmethod
    async def flow_acquire_lock(self, flow_id: UUID):
        """Create a lock for working on a flow. This ensures, there is no duplicate execution."""
        ...

    @abc.abstractmethod
    async def flow_release_lock(self, flow_id: UUID):
        """Create a lock for working on a flow. This ensures, there is no duplicate execution."""
        ...

    @abc.abstractmethod
    async def flow_get_cached_result(
        self, flow_key: str, parent_flow_id: UUID, parameters: SerializedParams
    ): ...

    @abc.abstractmethod
    async def flow_dispatch(
        self,
        flow_id: UUID,
        key: str,
        parameters: SerializedParams,
        scheduled: bool = False,
        retries: int = 0,
        parent_flow_id: UUID = None,
    ):
        """Dispatches a flow."""
        ...

    @abc.abstractmethod
    async def flow_acquire_one(
        self, flow_keys: list[str]
    ) -> tuple[str, UUID, SerializedParams]: ...

    @abc.abstractmethod
    async def flow_start_work(self, flow_id: UUID):
        """Indicates, that a worker started working on the flow"""
        ...

    @abc.abstractmethod
    async def flow_complete(self, flow_id: UUID, result: JSON):
        """Completes a flow. Status and result have to be updated, the flow must be removed from the heartbeat set."""
        ...

    @abc.abstractmethod
    async def flow_fail(self, flow_id: UUID, exception: Exception):
        """Marks a flow as failed. Status has to be updated, the flow must be removed from the heartbeat set and added to a failed set in one transcation."""
        ...

    @abc.abstractmethod
    async def write_flow_logs(
        self, items: tuple[Literal["stdout", "stderr"], UUID, datetime, str]
    ):
        """Write to the log stream of the flow."""
        ...
