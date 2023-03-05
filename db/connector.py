import sqlite3


class Connection:
    def __init__(self, db_string):
        self.db_string = db_string

    @property
    def _cursor(self):
        self._connection = sqlite3.connect(self.db_string)
        return self._connection.cursor()

    def exec(self, stmt: str, *args, **kwargs):
        if args or kwargs:
            self._cursor.executemany(stmt, *args, **kwargs)
        else:
            self._cursor.execute(stmt)
        self._connection.commit()
        self._connection.close()

    def select(self, stmt: str, mode: str):
        res = self._cursor.execute(stmt)
        if mode == "one":
            res = res.fetchone()
        elif mode == "all":
            res = res.fetchall()
        self._connection.close()
        return res
