
import datetime as dt
from pathlib import Path
from threading import Thread
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from sqlite3 import connect
from enum import Enum, auto

import requests

from broker.master import BrokerMaster
from checker_parsers import parse_from_kolejka


class RequestStatus(Enum):
    RECEIVED = auto()
    PENDING = auto()
    CHECKED = auto()
    SENDING_ERROR = auto()
    # DONE = auto()


class BacaApiReceiver:

    def __init__(self,
                 master: BrokerMaster,
                 database: Path,
                 server_ip: str = '127.0.0.1',
                 server_port: int = 8081,
                 baca_api_url: str = 'http://127.0.0.1/broker_api/'):
        self.server_ip: str = server_ip
        self.server_port: int = server_port
        self.baca_url: str = baca_api_url
        self.master: BrokerMaster = master
        self.database: Path = database
        self.con = connect(str(self.database.absolute()))
        self.server = ThreadingHTTPServer2(self, (self.server_ip, self.server_port), BacaApiHandler)
        self.thread = Thread(target=self.server.serve_forever)

    @property
    def is_alive(self):
        return self.thread.is_alive()

    def start(self):
        assert not self.is_alive
        self.thread.start()

    def stop(self):
        assert self.is_alive
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    @staticmethod
    def _generate_submit_id(course: str, submit_id: int):
        return course + str(submit_id)

    def insert(self,
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

    def check(self, request_id: str) -> None:
        cur = self.con.execute('SELECT package_path, submit_path FROM baca_requests WHERE id = ?',
                               (request_id,))
        package_path, submit_path = cur.fetchone()
        cur = self.con.execute('UPDATE baca_requests SET state = ?, mod_time = ? WHERE id = ?',
                               (RequestStatus.PENDING, dt.datetime.now(), request_id))
        if cur.rowcount != 1:
            self.con.rollback()
            raise Exception  # TODO
        try:
            self.master.new_submit(request_id, package_path, submit_path)
        except Exception as e:
            self.con.rollback()
            raise Exception(e)  # TODO
        self.con.commit()

    def send(self, request_id: str, result_path: Path) -> None:
        cur = self.con.execute('SELECT course, submit_id, state FROM baca_requests WHERE id = ?',
                               (request_id,))
        tmp = cur.fetchone()
        if tmp is None:
            raise Exception  # TODO

        course, submit_id, state = tmp
        if state not in [RequestStatus.CHECKED, RequestStatus.SENDING_ERROR]:
            raise Exception  # TODO

        message = parse_from_kolejka(result_path)
        r = requests.post(url=self.baca_url, json=dict(message))
        if r.status_code != 200:
            self.con.execute('UPDATE baca_requests SET state = ?, mod_time = ? WHERE id = ?',
                             (RequestStatus.SENDING_ERROR, dt.datetime.now(), request_id))
            self.con.commit()
            raise Exception  # TODO

        cur = self.con.execute('DELETE FROM baca_requests WHERE id = ?', (request_id,))
        assert cur.lastrowid == 1


class ThreadingHTTPServer2(ThreadingHTTPServer):

    def __init__(self, manager: BacaApiReceiver, *args, **kwargs):
        self.manager = manager
        super().__init__(*args, **kwargs)


class BacaApiHandler(BaseHTTPRequestHandler):

    def __init__(self, request: bytes, client_address: tuple[str, int], server: ThreadingHTTPServer2):
        super().__init__(request, client_address, server)
        self.server: ThreadingHTTPServer2 = server

    def do_POST(self):  # TODO
        ...
