import itertools

import psycopg
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from boedb.config import DBConfig


def get_db_client():
    if hasattr(PostgresClient, "_client"):
        return PostgresClient._client

    client = PostgresClient(DBConfig.DSN)
    PostgresClient._client = client
    return client


class PostgresClient:
    def __init__(self, dsn):
        self.dsn = dsn
        self.pool = ConnectionPool(dsn)
        self.async_pool = AsyncConnectionPool(dsn)

    def insert_many(self, table, row_dicts, columns=None):
        if columns is None:
            columns = set(itertools.chain.from_iterable(list(rd.keys()) for rd in row_dicts))

        rows = []
        for row_dict in row_dicts:
            row_values = tuple(row_dict.get(c) for c in columns)
            rows.append(row_values)

        with psycopg.connect(self.dsn) as conn:  # pylint: disable-all
            with conn.cursor() as cursor:
                columns_fmt = ", ".join(columns)
                values_fmt = ", ".join(r"%s" for c in columns)
                sql = f"INSERT INTO {table} ({columns_fmt}) VALUES ({values_fmt})"

                return cursor.executemany(sql, rows)

    def insert(self, table, row_dict, columns=None):
        return self.insert_many(table, [row_dict], columns)

    def execute(self, sql):
        with self.pool.connection() as conn:  # pylint: disable-all
            with conn.cursor() as cursor:
                yield from cursor.execute(sql)

    async def execute_async(self, sql):
        async with self.async_pool.connection() as conn:  # pylint: disable-all
            async with conn.cursor() as cursor:
                return await cursor.execute(sql)
