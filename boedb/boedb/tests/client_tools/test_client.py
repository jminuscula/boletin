import asyncio
import logging

from boedb.client import get_http_client_session

BASE_TEST_URL = "http://0.0.0.0:8080"


async def make_request(client):
    url = f"{BASE_TEST_URL}/status/502"
    async with client.get(url) as req:
        req.raise_for_status()
        return await req.text()


async def test_make_request():
    async with get_http_client_session() as client:
        async with asyncio.TaskGroup() as tg:
            task = tg.create_task(make_request(client))

        print(task.result())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(test_make_request())
