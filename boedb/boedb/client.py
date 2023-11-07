import asyncio
import re
import ssl
import urllib.parse

import aiohttp
import certifi

from boedb.config import HttpConfig, get_logger


def get_http_client_session():  # pragma: no cover
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_verify_locations(certifi.where())
    conn = aiohttp.TCPConnector(ssl=ctx)
    session = aiohttp.ClientSession(connector=conn)
    return session


class HttpRetryManager:
    def __init__(self, max_attempts):
        self.max_attempts = max_attempts
        self.req_attempts = {}
        self.logger = get_logger("http")

    async def retry_wait(self, request, response, req_id):
        """
        Wait appropriately and return True if the request should be retried,
        return False immediately otherwise. Wait time is determined by either
        response headers or incremental backoff per request.
        """
        next_attempt = self.req_attempts.get(req_id, 1)
        self.req_attempts[req_id] = next_attempt + 1
        if next_attempt < self.max_attempts:
            timeout = self.get_response_wait_time(request, response, req_id)
            if timeout:
                self.logger.debug(f"Waiting {timeout}s as per response headers")
            else:
                timeout = self.get_backoff_wait_time(next_attempt)
                self.logger.debug(f"Waiting {timeout}s for retry attempt {next_attempt}")
            await asyncio.sleep(timeout)
            return True

        return False

    @staticmethod
    def _parse_time_period(time_str):
        if not type(time_str) is str:
            return time_str

        if matches := re.findall(r"(\d+)(m?s)", time_str):
            t, unit = matches[0]
            return int(t) / 1000 if unit == "ms" else int(t)

    def get_response_wait_time(self, request, response, req_id):
        retry_after = response.headers.get("Retry-After", 0)
        if retry_after and not retry_after.isdigit():
            retry_after = 0

        if header_reset_s := max(
            int(retry_after),
            self._parse_time_period(response.headers.get("x-ratelimit-reset-requests", 0)),
            self._parse_time_period(response.headers.get("x-ratelimit-reset-tokens", 0)),
        ):
            return header_reset_s

    def get_backoff_wait_time(self, next_attempt):
        return HttpConfig.BASE_RETRY_WAIT_TIME * next_attempt


class HttpClient:
    def __init__(self, session, base_url=None, headers=None, timeout=None, retry_manager=None):
        self.session = session
        self.base_url = base_url
        self.headers = headers
        self.timeout = timeout or HttpConfig.REQUEST_TIMEOUT
        self.retry_manager = retry_manager
        if retry_manager is None:
            self.retry_manager = HttpRetryManager(HttpConfig.MAX_ATTEMPTS)
        self.logger = get_logger("http")

    def get_url(self, path):
        if self.base_url:
            return urllib.parse.urljoin(self.base_url, path)
        return path

    async def handle_request(self, req_params, req_id, parse_response=True):
        request = self.session.request(**req_params)
        async with request as response:
            body = None
            if not response.ok:
                body = await response.text()

            try:
                response.raise_for_status()
                if parse_response:
                    return await response.json()
                return await response.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                self.logger.warning(f"Request {req_id} error: ({exc})\n{body}")
                if self.retry_manager:
                    if await self.retry_manager.retry_wait(request, response, req_id):
                        return await self.handle_request(req_params, req_id, parse_response)

                self.logger.error(f"Request {req_id} aborted: max attempts exceeded")
                raise exc from None

    async def get(self, path, params=None, parse_response=True, req_id=None):
        req_id = req_id or hash(object())
        url = self.get_url(path)
        self.logger.debug(f"Request {req_id}: GET({url})")
        req_params = {
            "method": "get",
            "url": url,
            "params": params,
            "headers": self.headers,
            "timeout": self.timeout,
        }
        return await self.handle_request(req_params, req_id, parse_response)

    async def post(self, path, json, params=None, parse_response=True, req_id=None):
        req_id = req_id or hash(object())
        url = self.get_url(path)
        self.logger.debug(f"Request {req_id}: POST({url})")
        req_params = {
            "method": "post",
            "url": url,
            "params": params,
            "json": json,
            "headers": self.headers,
            "timeout": self.timeout,
        }
        return await self.handle_request(req_params, req_id, parse_response)
