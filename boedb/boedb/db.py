import itertools

import psycopg

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

    def insert_many(self, row_dicts, table, columns=None):
        if columns is None:
            columns = set(itertools.chain.from_iterable(list(rd.keys()) for rd in row_dicts))

        rows = []
        for row_dict in row_dicts:
            row_values = tuple(row_dict.get(c) for c in columns)
            rows.append(row_values)

        with psycopg.connect(self.dsn) as conn:  # pylint: disable-all
            with conn.cursor() as cursor:
                columns_fmt = ", ".join(self.columns)
                values_fmt = ", ".join(r"%s" for c in self.columns)
                sql = f"INSERT INTO {self.table} ({columns_fmt}) VALUES ({values_fmt})"

                return cursor.executemany(sql, rows)

    def insert(self, table, row_dict):
        return self.insert_many(table, [row_dict])

    def execute(self, sql):
        with psycopg.connect(self.dsn) as conn:  # pylint: disable-all
            with conn.cursor() as cursor:
                yield from cursor.execute(sql)
