from aiorun import run

from example import my_flow
from example import task1
from example import task2
from flive.backends.database import DatabaseBackend
from flive.orchestrator import Orchestrator

connection_string = (
    "postgresql+asyncpg://postgres:mysecretpassword@localhost:5432/flive"
)


async def main():
    backend = DatabaseBackend(connection_string)
    backend.activate()

    orchestrator = Orchestrator(flows=[task1, task2, my_flow])
    await orchestrator.run()


if __name__ == "__main__":
    run(main())
