import asyncio
import logging
import time

from boedb.client import HttpClient
from boedb.config import OpenAiConfig

COMPLETION_MODEL_NAME = "gpt-3.5-turbo-16k"
EMEDDINGS_MODEL_NAME = "text-embedding-ada-002"
BASE_URL = "https://api.openai.com/v1"


class OpenAiClient:
    def __init__(self, http_session):
        headers = {"Authorization": f"Bearer {OpenAiConfig.API_KEY}"}
        self.client = HttpClient(
            http_session, BASE_URL, headers=headers, timeout=OpenAiConfig.REQUEST_TIMEOUT
        )
        self.logger = logging.getLogger("boedb.openai")

    async def post(self, endpoint, payload):
        return await self.client.post(endpoint, payload)

    async def complete(self, prompt, max_tokens=None):
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
