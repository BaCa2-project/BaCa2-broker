import os
import shutil
import stat

import requests as req
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from threading import Thread, Lock

from db.connector import Connection
from .submit import TaskSubmit

from settings import KOLEJKA_SRC_DIR, APP_SETTINGS


class BrokerMaster:
    def __init__(self,
                 db_string: str,
                 submits_dir: Path,
                 delete_records: bool = APP_SETTINGS['delete_records'],
                 threads: int = 2,
                 server_address: tuple[str, int] = ('127.0.0.1', 15212)
                 ):
        self.connection = Connection(db_string)
        self.delete_records = delete_records
        self.submits_dir = submits_dir
        self.threads = threads
        self.submits = {}
        self.submit_http_server = KolejkaCommunicationServer(*server_address)
        self.submit_http_server.start_server()

    def __del__(self):
        self.submit_http_server.stop_server()

    @staticmethod
    def refresh_kolejka_src(add_executable_attr: bool = True):
        if KOLEJKA_SRC_DIR.is_dir():
            shutil.rmtree(KOLEJKA_SRC_DIR)
        KOLEJKA_SRC_DIR.mkdir()

        kolejka_judge = req.get('https://kolejka.matinf.uj.edu.pl/kolejka-judge').content
        kolejka_client = req.get('https://kolejka.matinf.uj.edu.pl/kolejka-client').content

        kolejka_judge_path = KOLEJKA_SRC_DIR / 'kolejka-judge'
        kolejka_client_path = KOLEJKA_SRC_DIR / 'kolejka-client'

        with open(kolejka_judge_path, mode='wb') as judge:
            judge.write(kolejka_judge)
        with open(kolejka_client_path, mode='wb') as client:
            client.write(kolejka_client)

        if add_executable_attr:
            current_judge = os.stat(kolejka_judge_path)
            current_client = os.stat(kolejka_client_path)

            os.chmod(kolejka_judge_path, current_judge.st_mode | stat.S_IEXEC)
            os.chmod(kolejka_client_path, current_client.st_mode | stat.S_IEXEC)

    def new_submit(self,
                   submit_id: str,
                   package_path: Path,
                   commit_id: str,
                   submit_path: Path):
        submit = TaskSubmit(self,
                            submit_id,
                            package_path,
                            commit_id,
                            submit_path,
                            force_rebuild=APP_SETTINGS['force_rebuild'],
                            verbose=APP_SETTINGS['verbose'])
        self.submits[submit_id] = submit
        submit.start()

    def close_submit(self, submit_id: str):
        if self.submits.get(submit_id) is not None:
            del self.submits[submit_id]


class KolejkaCommunicationServer:
    """
    Manages a http server that listens for updates from KOLEJKA system about submit records' statuses.
    Provides methods for awaiting calls from KOLEJKA system.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.server = ThreadingHTTPServer2(self, (host, port), _KolejkaCommunicationHandler)
        self.server_thread = Thread(target=self.server.serve_forever)
        self.submit_dict: dict[str, Lock] = {}
        self.integrity_lock = Lock()  # for protection against data races

    def __len__(self):
        with self.integrity_lock:
            length = len(self.submit_dict)
        return length

    def start_server(self) -> None:
        """Starts the HTTP server in a separate thread"""
        assert not self.is_active
        self.server_thread.start()

    def stop_server(self) -> None:
        """Stops the HTTP server"""
        assert self.is_active
        self.server.shutdown()
        self.server.server_close()

    @property
    def is_active(self) -> bool:
        """Returns True if the HTTP server is currently operational"""
        return self.server_thread.is_alive()

    def add_submit(self, submit_id: str) -> str:
        # TODO: adding submit should return url for submit
        """
        Adds a submit record to the local storage. Marks it as 'awaiting checking' for KOLEJKA system.

        :raise ValueError: if the submit record is already in the local storage
        """
        with self.integrity_lock:
            if submit_id in self.submit_dict:
                raise ValueError('Submit with id %s already registered.' % submit_id)
            self.submit_dict[submit_id] = Lock()
            self.submit_dict[submit_id].acquire()
        return f'http://{self.host}:{self.port}/{submit_id}'

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


class ThreadingHTTPServer2(ThreadingHTTPServer):
    """
    Exactly the same thing as ThreadingHTTPServer but with an additional attribute 'manager'.
    'manager' field stores KolejkaCommunicationServer instance so that the HTTP handler can invoke
    KolejkaCommunicationServer methods.
    """

    def __init__(self, manager: KolejkaCommunicationServer, *args, **kwargs):
        self.manager = manager
        super().__init__(*args, **kwargs)


class _KolejkaCommunicationHandler(BaseHTTPRequestHandler):
    """
    HTTP handler class for communication with KOLEJKA system
    """

    def __init__(self, request: bytes, client_address: tuple[str, int], server: ThreadingHTTPServer2):
        super().__init__(request, client_address, server)
        self.server: ThreadingHTTPServer2 = server

    def do_GET(self):  # TODO rewrite this entire method
        """Handles http requests."""
        manager: KolejkaCommunicationServer = self.server.manager
        submit_id = ''.join(filter(lambda x: x != '/', self.path))
        try:
            manager.release_submit(submit_id)
        except (KeyError, ValueError):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'F')
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'S')

    def log_message(self, format: str, *args) -> None:
        pass
