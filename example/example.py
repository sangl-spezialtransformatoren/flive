import asyncio

from aiorun import run

from flive import flow
from flive.backends.database import DatabaseBackend

connection_string = (
    "postgresql+asyncpg://postgres:mysecretpassword@localhost:5432/flive"
)


@flow(key="task1")
async def task1():
    print("Task 1")
    await asyncio.sleep(1)


@flow(key="task2")
async def task2() -> int:
    print("Task 2")
    await asyncio.sleep(30)
    return 1


@flow(key="my_flow")
async def my_flow():
    print("Starting my flow")
    await asyncio.sleep(10)
    print("Result of task 1", await task1())
    print("Result of task 2", await task2())
    print("Finished!")


async def main():
    backend = DatabaseBackend(connection_string)
    backend.activate()
    result = await my_flow.delay()
    print(result)
    asyncio.get_event_loop().stop()


if __name__ == "__main__":
    run(main())
