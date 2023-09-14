import ssl

import aiohttp
import certifi


def get_http_client_session():  # pragma: no cover
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_verify_locations(certifi.where())
    conn = aiohttp.TCPConnector(ssl=ctx)
    return aiohttp.ClientSession(connector=conn)
