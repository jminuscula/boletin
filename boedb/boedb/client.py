import logging
import ssl

import aiohttp
import certifi
from aiohttp_retry import ExponentialRetry, RetryClient

RETRY_MAX_ATTEMPTS = 3


class ExponentialRateLimitRetry(ExponentialRetry):
    def get_timeout(self, attempt, response):
        retry_after = response.headers.get("Retry-After", 0)
        if retry_after and not retry_after.isdigit():
            retry_after = 0

        if header_reset_s := max(
            int(retry_after),
            int(response.headers.get("x-ratelimit-reset-requests", 0)),
            int(response.headers.get("x-ratelimit-reset-tokens", 0)),
        ):
            return header_reset_s

        return super().get_timeout(attempt, response)


async def on_request_start(session, config_ctx, params):
    logging.getLogger("http").debug(f"[{params.method}] {params.url}")


async def on_request_exception(session, config_ctx, params):
    msg = f"Request error {params.method}({params.url}): {params.exception}"
    logging.getLogger("http").error(msg)


async def on_request_end(session, config_ctx, params):
    # aiohttp_retry will not trace exceptions for retry requests, so
    # we are forced to trace all request end events.
    if not params.response.ok:
        attempt = config_ctx.trace_request_ctx.get("current_attempt", None)
        logger = logging.getLogger("http")

        attempt_msg = f"Attempt {attempt}/{RETRY_MAX_ATTEMPTS}." if attempt else ""
        logger.warning(f"Request error {params.method}({params.url}). {attempt_msg}")


def get_http_client_session():  # pragma: no cover
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_verify_locations(certifi.where())
    conn = aiohttp.TCPConnector(ssl=ctx)

    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_start.append(on_request_start)
    trace_config.on_request_exception.append(on_request_exception)
    trace_config.on_request_end.append(on_request_end)

    # Requests can't raise for status automatically, otherwise aiohttp_retry would
    # not retry them. Instead, raise for status manually on each response.
    session = aiohttp.ClientSession(connector=conn, trace_configs=[trace_config])
    backoff = ExponentialRateLimitRetry(attempts=RETRY_MAX_ATTEMPTS)
    client = RetryClient(client_session=session, retry_options=backoff)

    # suppress retry debug messages
    logging.getLogger("aiohttp_retry").setLevel(logging.INFO)

    return client
