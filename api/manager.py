
from typing import Callable, Any
import datetime as dt
from pathlib import Path
from sqlite3 import connect
from enum import Enum, auto

import requests

from .message import BrokerToBaca


class RequestStatus(Enum):
    RECEIVED = auto()
    PENDING = auto()
    CHECKED = auto()
    SENDING_ERROR = auto()


class BacaApiManager:

    def __init__(self,
                 check_submit: Callable[[str, Path, Path], Any],
                 database: Path,
                 baca_api_url: str = 'http://127.0.0.1/broker_api/'):
        """
        Api class for receiving and sending requests from baca main server.

        :param check_submit: method or function which should be called to check submit
        :param database: sqlite3 database file containing `baca_requests` table
        :param baca_api_url: url where checking results should be sent in json format
        """
        self.baca_url: str = baca_api_url
        self.send_submit: Callable[[str, Path, Path], Any] = check_submit
        self.database: Path = database
        self.con = connect(str(self.database.absolute()))

    @staticmethod
    def _generate_submit_id(course: str, submit_id: int):
        return course + ':' + str(submit_id)

    def _insert(self,
                course: str,
                submit_id: int,
                package_path: str,
                solution_path: str) -> str:
        """
        :return: request_id, which is used as `id` in both `submit_records` and `baca_requests` tables
        """
        request_id = self._generate_submit_id(course, submit_id)
        cur = self.con.execute(
            '''
            INSERT INTO
                baca_requests(id, course, submit_id, submit_path, package_path, mod_time, state)
            VALUES
                (?, ?, ?, ?, ?, ?, ?)
            ''',
            (request_id, course, submit_id, solution_path, package_path,
             dt.datetime.now(), RequestStatus.RECEIVED)
        )
        if cur.rowcount != 1:
            self.con.rollback()
            raise Exception  # TODO
        self.con.commit()
        return request_id

    def _check(self, request_id: str) -> None:
        cur = self.con.execute('SELECT package_path, submit_path, state FROM baca_requests WHERE id = ?',
                               (request_id,))
        tmp = cur.fetchone()
        if tmp is None:
            raise Exception  # TODO

        package_path, submit_path, state = tmp
        if state != RequestStatus.RECEIVED:
            raise Exception  # TODO

        cur = self.con.execute('UPDATE baca_requests SET state = ?, mod_time = ? WHERE id = ?',
                               (RequestStatus.PENDING, dt.datetime.now(), request_id))
        if cur.rowcount != 1:
            self.con.rollback()
            raise Exception  # TODO
        try:
            self.send_submit(request_id, package_path, submit_path)
        except Exception as e:
            self.con.rollback()
            raise Exception(e)  # TODO
        self.con.commit()

    def receive(self,
                course: str,
                submit_id: int,
                package_path: str,
                solution_path: str) -> None:
        """
        Creates a new entry in database and sends it to the checker using self.check_submit(...)
        """
        request_id = self._insert(course, submit_id, package_path, solution_path)
        self._check(request_id)

    def send(self, request_id: str, result_path: Path, parser: Callable[[Path], BrokerToBaca]) -> None:
        cur = self.con.execute('SELECT course, submit_id, state FROM baca_requests WHERE id = ?',
                               (request_id,))
        tmp = cur.fetchone()
        if tmp is None:
            raise Exception  # TODO

        course, submit_id, state = tmp
        if state not in [RequestStatus.CHECKED, RequestStatus.SENDING_ERROR]:
            raise Exception  # TODO

        message = parser(result_path)
        r = requests.post(url=self.baca_url, json=dict(message))
        if r.status_code != 200:
            self.con.execute('UPDATE baca_requests SET state = ?, mod_time = ? WHERE id = ?',
                             (RequestStatus.SENDING_ERROR, dt.datetime.now(), request_id))
            self.con.commit()
            raise Exception  # TODO

        cur = self.con.execute('DELETE FROM baca_requests WHERE id = ?', (request_id,))
        assert cur.lastrowid == 1
