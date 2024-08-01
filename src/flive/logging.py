import asyncio
from asyncio import CancelledError
from datetime import UTC
from datetime import datetime
from io import TextIOBase
from queue import Queue

from flive.backends.common import get_current_backend


class MultiOutputStream(TextIOBase):
    def __init__(self, *streams):
        super().__init__()
        self.streams = streams

    def write(self, s):
        for stream in self.streams:
            stream.write(s)
        return len(s)

    def flush(self):
        for stream in self.streams:
            stream.flush()

    def close(self):
        for stream in self.streams:
            stream.close()


class FliveStdOut:
    streamer: "FliveStreamer"

    def __init__(self, streamer: "FliveStreamer") -> None:
        self.streamer = streamer

    def write(self, msg):
        from flive.flow import current_flow

        flow_id = current_flow.get()
        if flow_id:
            self.streamer.queue.put(("stdout", flow_id, datetime.now(tz=UTC), msg))

    def flush(self):
        pass

    def close(self):
        pass


class FliveStdErr:
    streamer: "FliveStreamer"

    def __init__(self, streamer: "FliveStreamer") -> None:
        self.streamer = streamer

    def write(self, msg):
        from flive.flow import current_flow

        flow_id = current_flow.get()
        if flow_id:
            self.streamer.queue.put(("stderr", flow_id, datetime.now(tz=UTC), msg))

    def flush(self):
        pass

    def close(self):
        pass


class FliveStreamer:
    queue: Queue

    def __init__(self):
        self.queue = Queue()

    @property
    def stdout(self) -> FliveStdOut:
        return FliveStdOut(self)

    @property
    def stderr(self) -> FliveStdErr:
        return FliveStdErr(self)

    async def work(self):
        backend = get_current_backend()
        BATCH_SIZE = 1000

        while True:
            try:
                size = self.queue.qsize()
                if size > BATCH_SIZE:
                    items = [self.queue.get_nowait() for _ in range(BATCH_SIZE)]
                else:
                    await asyncio.sleep(1)
                    if self.queue.qsize() == 0:
                        continue

                    items = [
                        self.queue.get_nowait()
                        for _ in range(0, min(self.queue.qsize(), BATCH_SIZE))
                    ]
                await backend.write_flow_logs(items)
            except CancelledError:
                break
