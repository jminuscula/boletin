import logging
import ssl

import aiohttp
import certifi


async def on_request_start(session, context, params):
    logging.getLogger("http").debug(f"[{params.method}] {params.url}")


def get_http_client_session():  # pragma: no cover
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_verify_locations(certifi.where())
    conn = aiohttp.TCPConnector(ssl=ctx)

    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_start.append(on_request_start)

    return aiohttp.ClientSession(connector=conn, trace_configs=[trace_config])
