
import datetime as dt
from pathlib import Path
from threading import Thread
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import http.client as client
from sqlite3 import connect
from enum import Enum, auto

from broker.master import BrokerMaster


class RequestStatus(Enum):
    ERROR = auto()
    RECEIVED = auto()
    PENDING = auto()
    CHECKED = auto()
    DONE = auto()


class BacaApiReceiver:

    def __init__(self, master: BrokerMaster, database: Path, ip: str = '127.0.0.1', port: int = 8081):
        self.ip: str = ip
        self.port: int = port
        self.master: BrokerMaster = master
        self.database: Path = database
        self.con = connect(str(self.database.absolute()))
        self.server = ThreadingHTTPServer2(self, (self.ip, self.port), BacaApiHandler)
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
               solution_path: str,
               output_path: str) -> str:
        """
        :return: request_id, which is used as `id` in both `submit_records` and `baca_requests` tables
        """
        request_id = self._generate_submit_id(course, submit_id)
        cur = self.con.execute(
            '''
            INSERT INTO
                baca_requests(id, course, submit_id, submit_path, package_path, result_path, mod_time, state)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (request_id, course, submit_id, solution_path, package_path,
             output_path, dt.datetime.now(), RequestStatus.RECEIVED)
        )
        if cur.rowcount != 1:
            self.con.rollback()
            raise Exception  # TODO
        self.con.commit()
        return request_id

    def check(self, request_id: str):
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

    def send(self, request_id: str):  # TODO
        ...


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
