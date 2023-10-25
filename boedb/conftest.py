from unittest import mock
from aiohttp import ClientSession

import pytest

from contextlib import asynccontextmanager


@pytest.fixture
def http_session_mock():
    async_ctx = mock.AsyncMock()
    async_ctx.__aenter__.return_value = mock.AsyncMock()

    session = mock.Mock()
    session.post = mock.Mock(return_value=async_ctx)

    return session
