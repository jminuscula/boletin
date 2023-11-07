from unittest import mock

import aiohttp
import pytest
from aioresponses import aioresponses

from boedb.client import HttpClient, HttpRetryManager, get_http_client_session


@pytest.mark.asyncio
@mock.patch("asyncio.sleep", return_value=mock.AsyncMock())
async def test_retry_manager_does_not_retry_past_max_attempts(sleep_mock):
    req_id = 1
    manager = HttpRetryManager(2)

    with mock.patch.object(manager, "get_response_wait_time", return_value=True):
        should_retry_one = await manager.retry_wait("request", "response", req_id)
        should_retry_two = await manager.retry_wait("request", "response", req_id)

    assert should_retry_one is True
    assert should_retry_two is False


def test_retry_manager_handles_period_ms():
    manager = HttpRetryManager(1)
    wait_seconds = manager._parse_time_period("1ms")
    assert wait_seconds == 1 / 1000


def test_retry_manager_handles_period_s():
    manager = HttpRetryManager(1)
    wait_seconds = manager._parse_time_period("1s")
    assert wait_seconds == 1


def test_retry_manager_gets_response_wait_time_from_header():
    req_id = 1
    manager = HttpRetryManager(1)

    response = mock.Mock(headers={"Retry-After": "1"})
    assert manager.get_response_wait_time("req", response, req_id) == 1

    response = mock.Mock(headers={"x-ratelimit-reset-requests": "10ms"})
    assert manager.get_response_wait_time("req", response, req_id) == 10 / 1000

    response = mock.Mock(headers={"x-ratelimit-reset-tokens": "10s"})
    assert manager.get_response_wait_time("req", response, req_id) == 10


def test_retry_manager_gets_max_response_wait_time_from_header():
    req_id = 1
    manager = HttpRetryManager(1)
    response = mock.Mock(
        headers={"Retry-After": "2023-11-05T10:52:33", "x-ratelimit-reset-requests": "2000ms"}
    )
    assert manager.get_response_wait_time("req", response, req_id) == 2


def test_retry_manager_wait_time_is_none_if_no_wait_header():
    req_id = 1
    manager = HttpRetryManager(1)
    response = mock.Mock(headers={})
    assert manager.get_response_wait_time("req", response, req_id) is None


@pytest.mark.asyncio
@mock.patch("asyncio.sleep", return_value=mock.AsyncMock())
@mock.patch("boedb.client.HttpConfig.BASE_RETRY_WAIT_TIME", 10)
async def test_retry_manager_gets_backoff_for_request(sleep_mock):
    req_id = 1
    manager = HttpRetryManager(3)
    response = mock.Mock(headers={})

    await manager.retry_wait("req", response, req_id)
    sleep_mock.assert_awaited_once_with(10)

    await manager.retry_wait("req", response, req_id)
    sleep_mock.assert_awaited_with(20)


@mock.patch("boedb.client.HttpConfig.REQUEST_TIMEOUT", 123)
@mock.patch("boedb.client.HttpConfig.MAX_ATTEMPTS", 321)
def test_http_client_inits_default_options():
    retry_manager_mock = mock.Mock()
    with mock.patch("boedb.client.HttpRetryManager") as ManagerMock:
        ManagerMock.return_value = retry_manager_mock
        client = HttpClient("manager")

    assert client.timeout == 123
    assert client.retry_manager is retry_manager_mock
    ManagerMock.assert_called_once_with(321)


def test_http_client_builds_url_with_base_url():
    base_url = "https://www.example.com"
    path = "/api/v1/path"
    client = HttpClient("manager", base_url)
    assert client.get_url(path) == "https://www.example.com/api/v1/path"


def test_http_client_builds_url_without_base_url():
    url = "https://www.example.com/api/v1/path"
    client = HttpClient("manager")
    assert client.get_url(url) == "https://www.example.com/api/v1/path"


@pytest.mark.asyncio
async def test_http_client_handles_get_response_ok():
    test_url = "https://test.com"
    response_body = "ok"
    with aioresponses() as mock_server:
        mock_server.get(test_url, status=200, body=response_body)

        async with get_http_client_session() as session:
            client = HttpClient(session, retry_manager=False)

            body = await client.get(test_url, parse_response=False)
            assert body == response_body


@pytest.mark.asyncio
async def test_http_client_handles_post_response_ok():
    test_url = "https://test.com"
    response_body = {"created": True}
    with aioresponses() as mock_server:
        mock_server.post(test_url, status=201, payload=response_body)

        async with get_http_client_session() as session:
            client = HttpClient(session, retry_manager=False)

            body = await client.post(test_url, json={}, parse_response=True)
            assert body == response_body


@pytest.mark.asyncio
async def test_http_client_handles_response_error():
    test_url = "https://test.com"
    with aioresponses() as mock_server:
        mock_server.get(test_url, status=429, body="error")

        async with get_http_client_session() as session:
            client = HttpClient(session, retry_manager=False)

            with pytest.raises(aiohttp.ClientError):
                await client.get(test_url)

            with pytest.raises(aiohttp.ClientError):
                await client.post(test_url, json={})


@pytest.mark.asyncio
async def test_http_client_handles_request_retry():
    test_url = "https://test.com"
    retry_manager = HttpRetryManager(2)
    retry_mock = mock.AsyncMock(wraps=retry_manager)
    retry_mock.retry_wait.side_effect = [True, False]

    with aioresponses() as mock_server:
        mock_server.get(test_url, status=429, body="error")
        mock_server.get(test_url, status=429, body="error")

        async with get_http_client_session() as session:
            client = HttpClient(session, retry_manager=retry_mock)

            with pytest.raises(aiohttp.ClientError):
                await client.get(test_url)
                retry_mock.retry_wait.assert_awaited_once()

        request_calls, *_ = list(mock_server.requests.values())
        assert len(request_calls) == 2
