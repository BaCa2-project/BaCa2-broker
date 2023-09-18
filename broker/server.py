from __future__ import annotations

from typing import TYPE_CHECKING
from threading import Lock, Thread
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import cgi
import json

from settings import APP_SETTINGS, BROKER_PASSWORD
from baca2PackageManager.broker_communication import *

if TYPE_CHECKING:
    from broker.master import BrokerMaster


class KolejkaCommunicationManager:
    """
    Manages a http server that listens for updates from KOLEJKA system about submit records' statuses.
    Provides methods for awaiting calls from KOLEJKA system.
    """

    def __init__(self):
        self.submit_dict: dict[str, Lock] = {}
        self.integrity_lock = Lock()  # for protection against data races

    def __len__(self):
        with self.integrity_lock:
            length = len(self.submit_dict)
        return length

    def add_submit(self, submit_id: str) -> None:
        """
        Adds a submit record to the local storage. Marks it as 'awaiting checking' for KOLEJKA system.

        :raise ValueError: if the submit record is already in the local storage
        """
        with self.integrity_lock:
            if submit_id in self.submit_dict:
                raise ValueError('Submit with id %s already registered.' % submit_id)
            self.submit_dict[submit_id] = Lock()
            self.submit_dict[submit_id].acquire()

    def release_submit(self, submit_id: str) -> None:
        """
        Marks a submit record as 'checked'.

        :raise KeyError: if the submit record is not present in the local storage
        :raise ValueError: if the submit record has already been released
        """
        with self.integrity_lock:
            if self.submit_dict[submit_id].locked():
                self.submit_dict[submit_id].release()
            else:
                raise ValueError('Submit with id %s has already been released.' % submit_id)

    def delete_submit(self, submit_id: str) -> None:
        """
        Removes a submit record from the local storage.

        raise KeyError: if the submit record is not present in the local storage
        """
        with self.integrity_lock:
            del self.submit_dict[submit_id]

    def await_submit(self, submit_id: str, timeout: float = -1) -> bool:
        """
        Returns True if a record's status changes to 'checked' within 'timeout' seconds after
        calling this method. If 'timeout' is a negative number waits indefinitely.

        raise KeyError: if the submit record is not present in the local storage
        """
        with self.integrity_lock:
            lock = self.submit_dict[submit_id]
        lock_acquired = lock.acquire(timeout=timeout)
        if lock_acquired:
            lock.release()
            try:
                self.delete_submit(submit_id)
            except KeyError:  # in case this method is called multiple times simultaneously for the same submit record
                pass
        return lock_acquired


class BrokerIOServer:

    def __init__(self,
                 kolejka_manager: KolejkaCommunicationManager,
                 broker_master: BrokerMaster
                 ):
        self.ip = APP_SETTINGS['server_ip']
        self.port = APP_SETTINGS['server_port']
        self.server = ThreadingHTTPBrokerServer(kolejka_manager,
                                                broker_master,
                                                server_address=(self.ip, self.port),
                                                RequestHandlerClass=BrokerServerHandler
                                                )
        self.server_thread = Thread(target=self.server.serve_forever)
        self.server_thread.start()

    def close_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join()

    def __del__(self):
        self.close_server()

    @classmethod
    def get_kolejka_callback_url(cls, submit_id: str) -> str:
        return f'http://{cls.ip}:{cls.port}/{BrokerServerHandler.KOLEJKA_PATH}/{submit_id}'


class ThreadingHTTPBrokerServer(ThreadingHTTPServer):

    def __init__(self,
                 kolejka_manager: KolejkaCommunicationManager,
                 broker_master: BrokerMaster,
                 *args, **kwargs):
        self.kolejka_manager = kolejka_manager
        self.broker_master = broker_master
        super().__init__(*args, **kwargs)

    @property
    def port(self):
        return self.server_port

    @property
    def address(self):
        return self.server_address


class BrokerServerHandler(BaseHTTPRequestHandler):

    KOLEJKA_PATH = 'kolejka'
    BACA_PATH = 'baca'

    def __init__(self, request: bytes, client_address: tuple[str, int], server: ThreadingHTTPBrokerServer):
        super().__init__(request, client_address, server)
        self.server: ThreadingHTTPBrokerServer = server

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        path = self.path.split('/')[1:]
        suffix = path[1:]
        match path[0]:
            case self.KOLEJKA_PATH:
                self.kolejka_post(suffix)
            case self.BACA_PATH:
                self.baca_post(suffix)
            case _:
                self.send_response(404)
                self.end_headers()

    def kolejka_post(self, suffix: list[str]):
        manager: KolejkaCommunicationManager = self.server.kolejka_manager
        submit_id = suffix[0]
        if not submit_id.isalnum():
            self.send_response(400)
            self.end_headers()
            return
        try:
            manager.release_submit(submit_id)
            self.send_response(200)
        except (KeyError, ValueError):
            self.send_response(400)
        self.end_headers()

    def baca_post(self, suffix: list[str]):
        type_, pdict = cgi.parse_header(self.headers.get('content-type'))

        if type_ != 'application/json':
            self.send_response(400)
            self.end_headers()
            return

        length = int(self.headers.get('content-length'))

        try:
            message = json.loads(self.rfile.read(length))
            content = BacaToBroker.parse(message)
        except Exception as e:
            self.wfile.write(str(e).encode('utf-8'))
            self.send_response(400)
            self.end_headers()
            return

        if make_hash(BROKER_PASSWORD, content.submit_id) != content.pass_hash:
            self.wfile.write('Wrong Password.'.encode('utf-8'))
            self.send_response(401)
            self.end_headers()
            return

        self.server.broker_master.new_submit(content.submit_id,
                                             content.package_path,
                                             content.commit_id,
                                             content.submit_path)
        self.send_response(200)
        self.end_headers()
