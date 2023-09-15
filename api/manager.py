from typing import Callable
from dataclasses import asdict
import datetime as dt
from pathlib import Path
from sqlite3 import connect

import requests

from baca2PackageManager.broker_communication import *


class RequestStatus:
    RECEIVED = 1
    CHECKED = 2
    SENDING_ERROR = 3


class BacaApiManager:

    def __init__(self,
                 result_parser: Callable[[str, int, Path], BrokerToBaca],
                 database: Path,
                 baca_api_url: str = 'http://127.0.0.1/broker_api/'):
        """
        Api class for receiving and sending requests from baca main server.

        :param database: sqlite3 database file containing `baca_requests` table
        :param baca_api_url: url where checking results should be sent in json format
        """
        self.baca_url: str = baca_api_url
        self.parser: Callable[[str, int, Path], BrokerToBaca] = result_parser
        self.database: Path = database.absolute()

    @staticmethod
    def _generate_submit_id(course: str, submit_id: int):
        return course + ':' + str(submit_id)

    def insert(self, message: BacaToBroker) -> str:
        """
        :return: request_id, which is used as `id` in both `submit_records` and `baca_requests` tables
        """
        con = connect(self.database)
        request_id = self._generate_submit_id(message.course_name, message.submit_id)
        cur = con.execute(
            '''
            INSERT INTO
                baca_requests(id, course, submit_id, submit_path, package_path, mod_time, state)
            VALUES
                (?, ?, ?, ?, ?, ?, ?)
            ''',
            (request_id, message.course_name, message.submit_id, str(message.solution_path), str(message.package_path),
             dt.datetime.now(), RequestStatus.RECEIVED)
        )
        assert cur.rowcount == 1
        con.commit()
        con.close()
        return request_id

    def _mark_checked(self, request_id: str) -> None:
        con = connect(self.database)
        cur = con.execute('SELECT package_path, submit_path, state FROM baca_requests WHERE id = ?',
                          (request_id,))
        tmp = cur.fetchone()
        if tmp is None:
            con.close()
            raise KeyError(f"No entry for id '{request_id}'.")

        # package_path, submit_path, state = tmp
        # if state != RequestStatus.RECEIVED:
        #     con.close()
        #     raise ValueError("Entry has to be marked as 'RECEIVED' in order to be checked.")

        cur = con.execute('UPDATE baca_requests SET state = ?, mod_time = ? WHERE id = ?',
                          (RequestStatus.CHECKED, dt.datetime.now(), request_id))
        assert cur.rowcount == 1
        con.commit()
        con.close()

    def _send(self, request_id: str, result_path: Path) -> None:
        con = connect(self.database)
        cur = con.cursor()
        cur.execute('SELECT course, submit_id, state FROM baca_requests WHERE id = ?',
                    (request_id,))
        tmp = cur.fetchone()
        if tmp is None:
            con.close()
            raise KeyError(f"No entry for id '{request_id}'.")

        course, submit_id, state = tmp
        if state not in [RequestStatus.CHECKED, RequestStatus.SENDING_ERROR]:
            con.close()
            raise ValueError("Entry has to be marked as 'CHECKED' or 'SENDING_ERROR' in order to be send.")

        message = self.parser(course, submit_id, result_path)
        r = requests.post(url=f'{self.baca_url}/result/{course}/{submit_id}', json=asdict(message))
        if r.status_code != 200:
            cur.execute('UPDATE baca_requests SET state = ?, mod_time = ? WHERE id = ?',
                        (RequestStatus.SENDING_ERROR, dt.datetime.now(), request_id))
            con.commit()
            con.close()
            raise ConnectionError(f"Results for entry with id {request_id} could not be send;"
                                  f" marked as SENDING_ERROR.")

        cur.execute('DELETE FROM baca_requests WHERE id = ?', (request_id,))
        assert cur.rowcount == 1
        con.commit()
        con.close()

    def mark_and_send(self, request_id: str, result_path: Path) -> None:
        self._mark_checked(request_id)
        self._send(request_id, result_path)
