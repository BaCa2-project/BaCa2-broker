import sqlite3
from pathlib import Path


class Connection:
    def __init__(self, db_string):
        self.db_string = db_string

    def truncate_db(self):
        self.exec('DELETE FROM submit_records')
        self.exec('DELETE FROM set_submit_records')

    @property
    def _cursor(self):
        self._connection = sqlite3.connect(self.db_string)
        return self._connection.cursor()

    @staticmethod
    def _translate(arg):
        import broker.submit
        if isinstance(arg, Path):
            arg = str(arg)
        elif isinstance(arg, broker.submit.SubmitState):
            arg = arg.value
        return arg

    @classmethod
    def _translate_args(cls, args):
        return [cls._translate(arg) for arg in args]

    def exec(self, stmt: str, *args):
        if args:
            self._cursor.execute(stmt, self._translate_args(args))
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
