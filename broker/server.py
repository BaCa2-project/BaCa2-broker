from __future__ import annotations

from typing import TYPE_CHECKING
from abc import ABC, abstractmethod
from threading import Thread
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import cgi
import json

from baca2PackageManager.broker_communication import *

if TYPE_CHECKING:
    from broker.master import BrokerMaster


class BacaApiServerAbstract(ABC):

    @abstractmethod
    def is_alive(self) -> bool:  ...

    @abstractmethod
    def start(self) -> None:  ...

    @abstractmethod
    def stop(self) -> None:  ...


class BacaApiServer(BacaApiServerAbstract):

    def __init__(self,
                 broker_master: BrokerMaster,
                 manager: BacaApiManager,
                 server_ip: str = '127.0.0.1',
                 server_port: int = 8180
                 ):
        self.broker_master = broker_master
        self.manager = manager
        self.server_ip: str = server_ip
        self.server_port: int = server_port
        self.server = ThreadingHTTPServer2(self, (self.server_ip, self.server_port), BacaApiHandler)
        self.thread = Thread(target=self.server.serve_forever)

    def is_alive(self):
        return self.thread.is_alive()

    def start(self):
        assert not self.is_alive()
        self.thread.start()

    def stop(self):
        assert self.is_alive()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class ThreadingHTTPServer2(ThreadingHTTPServer):

    def __init__(self,
                 broker_master: BrokerMaster,
                 manager: BacaApiServer,
                 *args, **kwargs):
        self.broker_master = broker_master
        self.manager = manager
        super().__init__(*args, **kwargs)


class BacaApiHandler(BaseHTTPRequestHandler):

    def __init__(self, request: bytes, client_address: tuple[str, int], server: ThreadingHTTPServer2):
        super().__init__(request, client_address, server)
        self.server: ThreadingHTTPServer2 = server

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        type_, pdict = cgi.parse_header(self.headers.get('content-type'))

        if type_ != 'application/json':
            self.send_response(400)
            self.end_headers()
            return

        length = int(self.headers.get('content-length'))
        message = json.loads(self.rfile.read(length))

        try:
            content = BacaToBroker(**message)
        except TypeError:  # TODO
            self.send_response(400)
            self.end_headers()
            return

        self.send_response(200)
        self.end_headers()
        # TODO: create submit id from course name and baca2 submit_id; use commit_id to identify package
        # FIXME: to delete: self.server.manager.manager.insert(content)
        self.server.broker_master.new_submit(new_submit_id,
                                             content.package_path,
                                             content.commit_id,
                                             content.submit_path)
        # TODO: Start checking process here
