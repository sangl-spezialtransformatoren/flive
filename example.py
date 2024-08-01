import asyncio

from flive import flow


@flow(name="test")
async def test(a: str) -> int:
    await asyncio.sleep(1)
    return 1


@test.on_commit
async def on_test_commit():
    print("Ok!")


async def main():
    x = await test(a="test")
    y = await test.delay(a="Y")
    print(x, y)


if __name__ == "__main__":
    asyncio.run(main())
