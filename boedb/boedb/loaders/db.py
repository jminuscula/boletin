import psycopg


class PostgresDocumentLoader:
    def __init__(self, dsn, table, columns):
        self.dsn = dsn
        self.table = table
        self.columns = columns

    def insert_many(self, cursor, iterable):
        rows = []
        for row_dict in iterable:
            row_values = tuple(row_dict.get(c) for c in self.columns)
            rows.append(row_values)

        columns_fmt = ", ".join(self.columns)
        values_fmt = ", ".join(r"%s" for c in self.columns)
        sql = f"INSERT INTO {self.table} ({columns_fmt}) VALUES ({values_fmt})"

        return cursor.executemany(sql, rows)

    def insert(self, cursor, row_dict):
        return self.insert_many(cursor, [row_dict])

    async def __call__(self, rows):
        with psycopg.connect(self.dsn) as conn:  # pylint: disable-all
            with conn.cursor() as curs:
                if isinstance(rows, list):
                    self.insert_many(curs, rows)
                else:
                    self.insert(curs, rows)
        return rows
