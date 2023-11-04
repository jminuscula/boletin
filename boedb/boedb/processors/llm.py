import asyncio
import logging
import time

from boedb.config import OpenAiConfig

COMPLETION_MODEL_NAME = "gpt-3.5-turbo-16k"
EMEDDINGS_MODEL_NAME = "text-embedding-ada-002"
BASE_URL = "https://api.openai.com/v1"


class OpenAiClient:
    def __init__(self, http_session):
        self.http_session = http_session
        self.logger = logging.getLogger("boedb.openai")

    async def post(self, endpoint, payload, attempt=1):
        start_time = time.time()
        headers = {"Authorization": f"Bearer {OpenAiConfig.API_KEY}"}
        request = self.http_session.post(
            endpoint, json=payload, headers=headers, timeout=OpenAiConfig.REQUEST_TIMEOUT
        )

        try:
            async with request as response:
                if not response.ok:
                    body = await response.text()
                    self.logger.error(f"Error on {endpoint}: {body}")
                return await response.json()

        # aiohttp-retry will only retry server generated exceptions. Timeouts and connection
        # errors need to be handled manually.
        except asyncio.TimeoutError as exc:
            et = time.time() - start_time
            if attempt < OpenAiConfig.REQUEST_MAX_RETRIES:
                self.logger.error(f"TimeoutError for OpenAI ({et:.2f}s), retrying x{attempt}.")
                return await self.post(endpoint, payload, attempt + 1)
            else:
                self.logger.error(f"TimeoutError for OpenAI ({et:.2f}s), max attempts reached.")
                raise exc from None

    async def complete(self, prompt, max_tokens=None, attempt=1):
        endpoint = f"{BASE_URL}/chat/completions"
        payload = {
            "model": COMPLETION_MODEL_NAME,
            "messages": prompt,
            "max_tokens": max_tokens,
        }

        data = await self.post(endpoint, payload)
        if choices := data.get("choices"):
            return choices[0]["message"]["content"].strip('"')

    async def get_embeddings(self, text):
        endpoint = f"{BASE_URL}/embeddings"
        payload = {
            "input": text,
            "model": EMEDDINGS_MODEL_NAME,
        }

        data = await self.post(endpoint, payload)
        if data := data.get("data"):
            return data[0]["embedding"]
