import asyncio
import logging

from boedb.client import HttpClient, get_http_client_session

BASE_TEST_URL = "http://0.0.0.0:8080"


async def make_request(client):
    url = f"{BASE_TEST_URL}/status/alternate"
    return await client.get(url, json=False)


async def test_make_request():
    async with get_http_client_session() as session:
        client = HttpClient(session)
        async with asyncio.TaskGroup() as tg:
            task = tg.create_task(make_request(client))

        try:
            print(task.result())
        except Exception as exc:
            print("error")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(test_make_request())
