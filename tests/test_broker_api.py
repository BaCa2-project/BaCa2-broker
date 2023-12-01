import cgi
import json
import os
import shutil
import unittest as ut
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from baca2PackageManager import *

from broker.submit import SubmitState
from broker.master import BrokerMaster
from settings import BASE_DIR

set_base_dir(BASE_DIR / 'tests' / 'test_packages')
add_supported_extensions('cpp')


KOLEJKA_CONFIGURED = False


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
        try:
            os.remove(self.test_dir / 'test.db')
        except Exception:  pass
        try:
            shutil.rmtree(self.test_dir / 'tmp_built')
        except Exception:  pass
        os.mkdir(self.test_dir / 'tmp_built')
        self.master = BrokerMaster(self.test_dir / 'test.db', self.test_dir / 'tmp_built', delete_records=True)
        os.system('sqlite3 {} <{}'.format(self.test_dir / 'test.db', self.test_dir.parent / 'db' / 'creator.sql'))

    def tearDown(self) -> None:
        self.master.stop()
        os.remove(self.test_dir / 'test.db')
        shutil.rmtree(self.test_dir / 'tmp_built')

    def test_cycle(self):
        self.master.new_submit('1',
                               self.test_dir / 'test_packages' / '1',
                               '1',
                               submit_path=self.test_dir / 'test_packages' / '1' / '1' / 'prog' / 'solution.cpp')
        submit = self.master.submits['1']
        submit.join()
        if KOLEJKA_CONFIGURED:
            self.assertEqual(SubmitState.DONE, submit.status)
        else:
            self.assertEqual(SubmitState.ERROR, submit.status)
