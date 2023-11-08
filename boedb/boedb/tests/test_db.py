from contextlib import contextmanager
from unittest import mock

import pytest

from boedb.config import DBConfig
from boedb.db import PostgresClient, get_db_client


@mock.patch("boedb.config.DBConfig.DSN", "dsn")
def test_get_db_client_returns_singleton():
    client_mock = mock.Mock()
    with mock.patch("boedb.db.PostgresClient", wraps=PostgresClient) as ClientMock:
        ClientMock.return_value = client_mock

        ins1 = get_db_client()
        ins2 = get_db_client()

    assert ins1 is ins2
    ClientMock.assert_called_once_with("dsn")


def test_postgres_client_executes_sql():
    test_rows = [1, 2, 3]
    conn_mock = mock.MagicMock()
    cursor_mock = mock.MagicMock()
    cursor_mock.execute.return_value = test_rows

    sql = "select * from test"
    with mock.patch("boedb.db.ConnectionPool") as PoolMock:
        PoolMock.return_value.connection.return_value.__enter__.return_value = conn_mock
        conn_mock.cursor.return_value.__enter__.return_value = cursor_mock

        client = PostgresClient("dsn")
        result = client.execute(sql)
        rows = list(result)

    cursor_mock.execute.assert_called_once_with(sql, None)
    assert rows == test_rows


def test_postgres_client_executes_sql_with_var():
    test_rows = [True]
    conn_mock = mock.MagicMock()
    cursor_mock = mock.MagicMock()
    cursor_mock.execute.return_value = test_rows

    sql = r"delete from test where id = %s"
    with mock.patch("boedb.db.ConnectionPool") as PoolMock:
        PoolMock.return_value.connection.return_value.__enter__.return_value = conn_mock
        conn_mock.cursor.return_value.__enter__.return_value = cursor_mock

        client = PostgresClient("dsn")
        result = client.execute(sql, 1)
        rows = list(result)

    cursor_mock.execute.assert_called_once_with(sql, 1)
    assert rows == test_rows


def test_postgres_client_executes_sql_with_vars():
    test_rows = [True, True, True]
    conn_mock = mock.MagicMock()
    cursor_mock = mock.MagicMock()
    cursor_mock.executemany.return_value = test_rows

    sql = r"delete from test where id = %s"
    with mock.patch("boedb.db.ConnectionPool") as PoolMock:
        PoolMock.return_value.connection.return_value.__enter__.return_value = conn_mock
        conn_mock.cursor.return_value.__enter__.return_value = cursor_mock

        client = PostgresClient("dsn")
        result = client.execute(sql, [1, 2, 3])
        rows = list(result)

    cursor_mock.executemany.assert_called_once_with(sql, [1, 2, 3])
    assert rows == test_rows


def test_postgres_client_inserts_row():
    table = "test_table"
    row_dict = {"column1": "value1", "colum2": "value2"}
    columns = ["column1", "column2"]
    with mock.patch("boedb.db.PostgresClient.insert_many") as insert_many_mock:
        client = PostgresClient("dsn")
        client.insert(table, row_dict, columns)

    insert_many_mock.assert_called_once_with(table, [row_dict], columns)


def test_postgres_client_inserts_many_with_columns():
    test_results = [True, True]
    table = "test_table"
    row_dict = {"column1": "value1", "column2": "value2"}
    columns = ["column1", "column2", "column3"]

    with mock.patch("boedb.db.PostgresClient.execute") as execute_mock:
        execute_mock.return_value = test_results
        client = PostgresClient("dsn")
        result = client.insert_many(table, [row_dict, row_dict], columns)

    insert_stm = r"INSERT INTO test_table (column1, column2, column3) VALUES (%s, %s, %s)"
    values = ("value1", "value2", None)
    execute_mock.assert_called_once_with(insert_stm, [values, values])
    assert result == test_results


def test_postgres_client_inserts_many_without_columns():
    test_results = [True, True]
    table = "test_table"
    row_dicts = [
        {"column1": "value1", "column2": "value2"},
        {"column2": "value2", "column3": "value3"},
    ]

    with mock.patch("boedb.db.PostgresClient.execute") as execute_mock:
        execute_mock.return_value = test_results
        client = PostgresClient("dsn")
        result = client.insert_many(table, row_dicts)

    insert_stm = r"INSERT INTO test_table (column1, column2, column3) VALUES (%s, %s, %s)"
    values = [("value1", "value2", None), (None, "value2", "value3")]
    execute_mock.assert_called_once_with(insert_stm, values)
    assert result == test_results
