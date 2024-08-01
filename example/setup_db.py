import asyncio
from datetime import timedelta

from sqlalchemy import insert
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from flive.backends.database import Base
from flive.backends.database import Settings

root_engine = create_async_engine(
    "postgresql+asyncpg://postgres:mysecretpassword@localhost:5432", echo=True
)

engine = create_async_engine(
    "postgresql+asyncpg://postgres:mysecretpassword@localhost:5432/flive", echo=True
)


async def main():
    async with root_engine.execution_options(
        isolation_level="AUTOCOMMIT"
    ).connect() as bind:
        await bind.execute(text("DROP DATABASE IF EXISTS flive WITH (FORCE);"))
        await bind.execute(text("CREATE database flive;"))

    async with engine.begin() as bind:
        await bind.run_sync(Base.metadata.create_all)
        await bind.execute(
            insert(Settings).values(
                id=0,
                heartbeat_interval=timedelta(seconds=30),
                orchestrator_dead_after=timedelta(minutes=3),
            )
        )


loop = asyncio.new_event_loop()
loop.run_until_complete(main())
