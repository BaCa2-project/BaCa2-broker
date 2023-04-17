from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from threading import Thread, Lock

from db.connector import Connection
from .submit import Submit


class BrokerMaster:
    def __init__(self,
                 db_string: str,
                 results_dir: Path,
                 delete_records: bool = True,
                 threads: int = 2,
                 server_address: tuple[str, int] = ('127.0.0.1', 8080)
                 ):
        self.connection = Connection(db_string)
        self.delete_records = delete_records
        self.results_dir = results_dir
        self.threads = threads
        self.submits = {}
        self.submit_http_server = KolejkaCommunicationServer(*server_address)
        self.submit_http_server.start_server()

    def __del__(self):
        self.submit_http_server.stop_server()

    def new_submit(self,
                   submit_id: str,
                   package_path: Path,
                   submit_path: Path):
        submit = Submit(self, submit_id, package_path, submit_path)
        self.submits[submit_id] = submit
        submit.start()

    def close_submit(self, submit_id: str):
        if self.submits.get(submit_id) is not None:
            del self.submits[submit_id]


class KolejkaCommunicationServer:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.server = ThreadingHTTPServer2(self, (host, port), _KolejkaCommunicationHandler)
        self.server_thread = Thread(target=self.server.serve_forever)
        self.submit_dict: dict[str, Lock] = {}
        self.integrity_lock = Lock()

    def start_server(self) -> None:
        self.server_thread.start()

    def stop_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def is_active(self) -> bool:
        return self.server_thread.is_alive()

    def add_submit(self, submit_id: str) -> None:
        with self.integrity_lock:
            if submit_id in self.submit_dict:
                raise ValueError('Submit with id %s already registered.' % submit_id)
            self.submit_dict[submit_id] = Lock()
            self.submit_dict[submit_id].acquire()

    def release_submit(self, submit_id: str) -> None:
        with self.integrity_lock:
            if self.submit_dict[submit_id].locked():
                self.submit_dict[submit_id].release()
            else:
                raise ValueError('Submit with id %s has already been released.' % submit_id)

    def await_submit(self, submit_id: str, timeout: float = -1) -> bool:
        with self.integrity_lock:
            lock = self.submit_dict[submit_id]
        lock_acquired = lock.acquire(timeout=timeout)
        if lock_acquired:
            with self.integrity_lock:
                del self.submit_dict[submit_id]
        return lock_acquired


class ThreadingHTTPServer2(ThreadingHTTPServer):
    """
    Exactly the same thing as ThreadingHTTPServer but with additional attribute 'manager'.
    'manager' field stores KolejkaCommunicationServer instance so that the HTTP handler can invoke
    KolejkaCommunicationServer methods.
    """

    def __init__(self, manager: KolejkaCommunicationServer, *args, **kwargs):
        self.manager = manager
        super().__init__(*args, **kwargs)


class _KolejkaCommunicationHandler(BaseHTTPRequestHandler):

    def __init__(self, request: bytes, client_address: tuple[str, int], server: ThreadingHTTPServer2):
        super().__init__(request, client_address, server)
        self.server: ThreadingHTTPServer2 = server

    def do_GET(self):  # TODO rewrite this entire method
        """
        Handles http requests
        """
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
