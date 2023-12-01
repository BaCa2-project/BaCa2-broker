import cgi
import json
import os
import unittest as ut
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread, Lock
from time import sleep

from baca2PackageManager import *

from broker.master import BrokerMaster
from settings import BASE_DIR

set_base_dir(BASE_DIR / 'tests' / 'test_packages')
add_supported_extensions('cpp')


class DummyBacaServer(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Content-Encoding', 'UTF-8')
        self.end_headers()

    def do_HEAD(self):
        self._set_headers()

    def do_GET(self):
        self._set_headers()
        self.wfile.write(json.dumps({'hello': 'world', 'received': 'ok'}))

    def do_POST(self):
        c_type, pdict = cgi.parse_header(self.headers.get('content-type'))

        if c_type != 'application/json':
            self.send_response(400)
            self.end_headers()
            return

        length = int(self.headers.get('content-length'))
        message = json.loads(self.rfile.read(length))

        # add a property to the object, just to mess with data
        message['received'] = 'ok'

        # send the message back
        self._set_headers()
#       print(message)


def server_run(server_class=HTTPServer, handler_class=DummyBacaServer, port=8180):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)

#   print('Starting httpd on port %d...' % port)
    th = Thread(target=httpd.serve_forever)
    th.start()
    return httpd, th


class BasicTests(ut.TestCase):
    test_dir = Path(__file__).absolute().parent

    def setUp(self) -> None:
        self.master = BrokerMaster(self.test_dir / 'test.db', BASE_DIR / 'tests' / 'test_packages', delete_records=True)
        os.system('sqlite3 {} <{}'.format(self.test_dir / 'test.db', self.test_dir.parent / 'db' / 'creator.sql'))

    def tearDown(self) -> None:
        os.remove(self.test_dir / 'test.db')

    def test_cycle(self):
        self.master.new_submit('1', self.test_dir / 'test_packages' / '1', '1', self.test_dir)

