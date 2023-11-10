from datetime import datetime
from unittest import mock

import psycopg
import pytest

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


def test_postgres_client_executes_sql_with_result():
    test_rows = [1, 2, 3]
    conn_mock = mock.MagicMock()
    cursor_mock = mock.MagicMock(__iter__=lambda s: iter(test_rows))
    cursor_mock.rownumber = 0

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
    conn_mock = mock.MagicMock()
    cursor_mock = mock.MagicMock()
    cursor_mock.execute.return_value = None
    cursor_mock.rownumber = None

    sql = r"delete from test where id = %s"
    with mock.patch("boedb.db.ConnectionPool") as PoolMock:
        PoolMock.return_value.connection.return_value.__enter__.return_value = conn_mock
        conn_mock.cursor.return_value.__enter__.return_value = cursor_mock

        client = PostgresClient("dsn")
        result = client.execute(sql, 1)

    cursor_mock.execute.assert_called_once_with(sql, 1)
    assert result is None


def test_postgres_client_executes_sql_with_var_with_result():
    test_rows = [1, 2, 3]
    conn_mock = mock.MagicMock()
    cursor_mock = mock.MagicMock(__iter__=lambda s: iter(test_rows))
    cursor_mock.rownumber = 0

    sql = r"select * from test where id = %s"
    with mock.patch("boedb.db.ConnectionPool") as PoolMock:
        PoolMock.return_value.connection.return_value.__enter__.return_value = conn_mock
        conn_mock.cursor.return_value.__enter__.return_value = cursor_mock

        client = PostgresClient("dsn")
        result = client.execute(sql, 1)
        rows = list(result)

    cursor_mock.execute.assert_called_once_with(sql, 1)
    assert rows == [1, 2, 3]


def test_postgres_client_executes_sql_with_vars():
    conn_mock = mock.MagicMock()
    cursor_mock = mock.MagicMock()
    cursor_mock.rownumber = None

    sql = r"delete from test where id = %s"
    with mock.patch("boedb.db.ConnectionPool") as PoolMock:
        PoolMock.return_value.connection.return_value.__enter__.return_value = conn_mock
        conn_mock.cursor.return_value.__enter__.return_value = cursor_mock

        client = PostgresClient("dsn")
        result = client.execute(
            sql,
            [
                (1,),
                (2,),
                (3,),
            ],
        )

    cursor_mock.executemany.assert_called_once_with(sql, [(1,), (2,), (3,)])
    assert result is None


def test_postgres_client_inserts_row():
    table = "test_table"
    row_dict = {"column1": "value1", "colum2": "value2"}
    columns = ["column1", "column2"]
    with mock.patch("boedb.db.PostgresClient.insert_many") as insert_many_mock:
        client = PostgresClient("dsn")
        client.insert(table, row_dict, columns)

    insert_many_mock.assert_called_once_with(table, [row_dict], columns)


def test_postgres_client_inserts_many_with_columns():
    table = "test_table"
    row_dict = {"column1": "value1", "column2": "value2"}
    columns = ["column1", "column2", "column3"]

    with mock.patch("boedb.db.PostgresClient.execute") as execute_mock:
        execute_mock.return_value = None
        client = PostgresClient("dsn")
        result = client.insert_many(table, [row_dict, row_dict], columns)

    insert_stm = r"INSERT INTO test_table (column1, column2, column3) VALUES (%s, %s, %s)"
    values = ("value1", "value2", None)
    execute_mock.assert_called_once_with(insert_stm, [values, values])
    assert result is None


def test_postgres_client_inserts_many_without_columns():
    table = "test_table"
    row_dicts = [
        {"column1": "value1", "column2": "value2"},
        {"column2": "value2", "column3": "value3"},
    ]

    with mock.patch("boedb.db.PostgresClient.execute") as execute_mock:
        execute_mock.return_value = None
        client = PostgresClient("dsn")
        result = client.insert_many(table, row_dicts)

    insert_stm = r"INSERT INTO test_table (column1, column2, column3) VALUES (%s, %s, %s)"
    values = [("value1", "value2", None), (None, "value2", "value3")]
    execute_mock.assert_called_once_with(insert_stm, values)
    assert result is None


@pytest.fixture
def test_db_cursor():
    with psycopg.connect("user=test dbname=test") as conn:
        conn.autocommit = True
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
            cursor.execute("CREATE TABLE test (id INT, data VARCHAR(64))")
            yield cursor
            cursor.execute(f"DROP TABLE test")


@pytest.fixture
def test_db_with_data_cursor(test_db_cursor):
    test_db_cursor.execute("INSERT INTO test (id, data) VALUES (1, 'value1')")
    test_db_cursor.execute("INSERT INTO test (id, data) VALUES (2, 'value2')")
    test_db_cursor.execute("INSERT INTO test (id, data) VALUES (3, 'value3')")
    yield test_db_cursor


@pytest.mark.integration
def test_db_client_can_insert_one(test_db_cursor):
    test_row = {"id": 10, "data": "test"}
    client = PostgresClient("user=test dbname=test")
    client.insert("test", test_row)

    test_db_cursor.execute("SELECT * FROM test")
    rows = test_db_cursor.fetchall()
    assert test_row in rows


@pytest.mark.integration
def test_db_client_can_insert_one_with_columns(test_db_cursor):
    test_row = {"id": 10, "data": "test"}
    client = PostgresClient("user=test dbname=test")
    client.insert("test", test_row, ("id", "data"))

    test_db_cursor.execute("SELECT * FROM test")
    rows = test_db_cursor.fetchall()
    assert test_row in rows


@pytest.mark.integration
def test_db_client_can_execute(test_db_with_data_cursor):
    client = PostgresClient("user=test dbname=test")
    rows = client.execute("SELECT * FROM test")
    assert rows == [
        {"id": 1, "data": "value1"},
        {"id": 2, "data": "value2"},
        {"id": 3, "data": "value3"},
    ]


@pytest.mark.integration
def test_db_client_can_execute_with_var(test_db_with_data_cursor):
    client = PostgresClient("user=test dbname=test")
    rows = client.execute(r"SELECT * FROM test WHERE id = %s", (1,))
    assert rows == [{"id": 1, "data": "value1"}]
