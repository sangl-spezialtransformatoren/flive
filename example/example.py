import asyncio
import os
import random
import sys
from datetime import timedelta

from aiorun import run
from loguru import logger
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import create_async_engine

from flive import FliveOrchestrator
from flive import flow
from flive.models import BaseModel
from flive.models import SettingsModel

connection_string = "sqlite+aiosqlite:///flive.db"

# Configure loguru to log debug messages to stdout
logger.remove()  # Remove default handler
logger.configure(extra={"flow_id": " "*8}) 
logger.add(sys.stdout, level="DEBUG", format="{time} | {level} | {extra[flow_id]} | {message}")


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
                heartbeat_interval=timedelta(seconds=5),
                orchestrator_dead_after=timedelta(seconds=30),
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
    result = await simple_task("World")
    print("!", result)


async def run_flow():
    orchestrator = FliveOrchestrator(connection_string)
    async with orchestrator:
        await main_flow()


if __name__ == "__main__":
    run(run_flow())
