import random

from boedb.config import OpenAiConfig

COMPLETION_MODEL_NAME = "gpt-3.5-turbo-16k"
EMEDDINGS_MODEL_NAME = "text-embedding-ada-002"
BASE_URL = "https://api.openai.com/v1"


class OpenAiClient:
    def __init__(self, http_session):
        self.http_session = http_session

    async def post(self, endpoint, payload):
        headers = {"Authorization": f"Bearer {OpenAiConfig.API_KEY}"}
        request = self.http_session.post(endpoint, json=payload, headers=headers)
        async with request as response:
            return await response.json()

    async def complete(self, prompt, max_tokens=None):
        endpoint = f"{BASE_URL}/chat/completions"
        payload = {
            "model": COMPLETION_MODEL_NAME,
            "messages": prompt,
            "max_tokens": max_tokens,
        }

        data = await self.post(endpoint, payload)
        if choices := data.get("choices"):
            return choices[0]["message"]["content"]

    async def get_embeddings(self, text):
        endpoint = f"{BASE_URL}/embeddings"
        payload = {
            "input": text,
            "model": EMEDDINGS_MODEL_NAME,
        }

        data = await self.post(endpoint, payload)
        if data := data.get("data"):
            return data[0]["embedding"]
