from contextlib import contextmanager
from unittest import mock

import pytest

from boedb.loaders.db import PostgresDocumentLoader


def test_insert_many_uses_target_table_and_schema():
    loader = PostgresDocumentLoader("dsn", "table", ("col1", "col2"))
    cursor = mock.Mock()
    articles = [{"col1": 1, "col2": 2}, {"col1": 1, "col2": 2}]

    loader.insert_many(cursor, articles)
    cursor.executemany.assert_called_once_with(r"INSERT INTO table (col1, col2) VALUES (%s, %s)", [(1, 2), (1, 2)])


def test_insert_calls_through_insert_many():
    loader = PostgresDocumentLoader("dsn", "table", ("col1", "col2"))
    cursor = mock.Mock()
    article = mock.Mock()

    with mock.patch.object(loader, "insert_many") as insert_many_mock:
        loader.insert(cursor, article)

    insert_many_mock.assert_called_once_with(cursor, [article])


@pytest.mark.asyncio
async def test_loader_inserts_many():
    articles = [mock.Mock()]
    loader = PostgresDocumentLoader("dsn", "table", ("col1", "col2"))

    cursor_mock = mock.Mock()

    @contextmanager
    def cursor_ctx_mock():
        yield cursor_mock

    @contextmanager
    def connect_ctx_mock():
        conn = mock.Mock()
        conn.cursor.return_value = cursor_ctx_mock()
        yield conn

    with mock.patch("boedb.loaders.db.psycopg.connect") as connect_mock:
        connect_mock.return_value = connect_ctx_mock()

        with mock.patch.object(loader, "insert_many") as insert_many_mock:
            await loader(articles)

    connect_mock.assert_called_once_with("dsn")
    insert_many_mock.assert_called_once_with(cursor_mock, articles)
