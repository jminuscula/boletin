import itertools
from collections.abc import Iterable

import psycopg
from psycopg_pool import ConnectionPool

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

    def execute(self, sql, vars=None):
        with self.pool.connection() as conn:  # pylint: disable-all
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                if isinstance(vars, Iterable) and len(vars) and isinstance(vars[0], Iterable):
                    cursor.executemany(sql, vars)
                else:
                    cursor.execute(sql, vars)
                if cursor.rownumber is not None:
                    return list(cursor)

    def insert(self, table, row_dict, columns=None):
        return self.insert_many(table, [row_dict], columns)

    def insert_many(self, table, row_dicts, columns=None):
        if columns is None:
            columns = set(itertools.chain.from_iterable(list(rd.keys()) for rd in row_dicts))
            columns = list(sorted(columns))

        values = []
        for row_dict in row_dicts:
            row_values = tuple(row_dict.get(c) for c in columns)
            values.append(row_values)

        columns_fmt = ", ".join(columns)
        values_fmt = ", ".join(r"%s" for c in columns)
        sql = f"INSERT INTO {table} ({columns_fmt}) VALUES ({values_fmt})"

        return self.execute(sql, values)
