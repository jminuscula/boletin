from unittest import mock

import pytest
from aioresponses import aioresponses

from boedb.client import get_http_client_session
from boedb.config import OpenAiConfig
from boedb.processors.llm import OpenAiClient


@pytest.mark.asyncio
@mock.patch("boedb.config.OpenAiConfig.API_KEY")
@mock.patch("boedb.processors.llm.HttpClient")
async def test_open_ai_client_post(HttpClientMock, api_key, http_session_mock):
    client_mock = mock.AsyncMock()
    HttpClientMock.return_value = client_mock
    client = OpenAiClient(http_session=http_session_mock)

    endpoint = "/test"
    payload = {"data": "test"}
    headers = {"Authorization": f"Bearer {api_key}"}

    await client.post(endpoint, payload)

    HttpClientMock.assert_called_once_with(
        http_session_mock, "https://api.openai.com/v1", headers=headers, timeout=OpenAiConfig.REQUEST_TIMEOUT
    )
    client_mock.post.assert_awaited_once_with(endpoint, payload)


@pytest.mark.asyncio
@mock.patch("boedb.config.OpenAiConfig.API_KEY")
async def test_open_ai_client_completes_returns_completion(api_key):
    endpoint = "https://api.openai.com/v1/chat/completions"
    test_completion = "test completion"
    test_response = {"choices": [{"message": {"content": test_completion}}]}
    test_prompt = [{"role": "user", "content": "test prompt"}]
    test_max_tokens = 100
    payload = {
        "model": "gpt-3.5-turbo-16k",
        "messages": test_prompt,
        "max_tokens": test_max_tokens,
    }

    http_session = get_http_client_session()
    client = OpenAiClient(http_session=http_session)

    with aioresponses() as mock_server:
        with mock.patch.object(client, "post", wraps=client.post) as client_post_mock:
            mock_server.post(endpoint, status=201, payload=test_response)
            completion = await client.complete(test_prompt, max_tokens=test_max_tokens)

    assert completion == test_completion
    client_post_mock.assert_awaited_once_with(endpoint, payload)


@pytest.mark.asyncio
@mock.patch("boedb.config.OpenAiConfig.API_KEY")
async def test_open_ai_client_get_embeddings(api_key):
    endpoint = "https://api.openai.com/v1/embeddings"
    test_input = "text"
    test_embeddings = [0.1, 0.2, 0.3]
    test_response = {"data": [{"embedding": test_embeddings}]}
    payload = {
        "input": test_input,
        "model": "text-embedding-ada-002",
    }

    http_session = get_http_client_session()
    client = OpenAiClient(http_session=http_session)

    with aioresponses() as mock_server:
        with mock.patch.object(client, "post", wraps=client.post) as client_post_mock:
            mock_server.post(endpoint, status=201, payload=test_response)
            embeddings = await client.get_embeddings(test_input)

    assert embeddings == test_embeddings
    client_post_mock.assert_awaited_once_with(endpoint, payload)
