import asyncio
import os
import random
from datetime import timedelta

from aiorun import run
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import create_async_engine

from flive import FliveOrchestrator
from flive import flow
from flive.orchestrator import BaseModel
from flive.orchestrator import SettingsModel

connection_string = "sqlite+aiosqlite:///flive.db"


async def setup_database():
    # Delete the existing database file if it exists
    if os.path.exists("flive.db"):
        os.remove("flive.db")

    engine = create_async_engine(connection_string)
    async with engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.create_all)
        await conn.execute(
            insert(SettingsModel).values(
                id=0,
                heartbeat_interval=timedelta(seconds=30),
                orchestrator_dead_after=timedelta(minutes=3),
            )
        )


@flow(key="simple_task", retries=3)
async def simple_task(name: str) -> str:
    await asyncio.sleep(2)  # Simulate some work
    if random.random() < 0.5:  # 50% chance of failure
        raise Exception("Random failure in simple_task")
    return f"Hello, {name}!"


@flow(key="main_flow")
async def main_flow():
    try:
        result = await simple_task("World")
        print("!", result)
    except Exception as e:
        print(f"Main flow caught an exception: {e}")


async def run_flow():
    orchestrator = FliveOrchestrator(connection_string)
    await orchestrator.run()


if __name__ == "__main__":
    run(run_flow())
