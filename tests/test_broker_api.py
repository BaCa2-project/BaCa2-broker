import sqlite3
import unittest as ut
import os
from pathlib import Path
from threading import Thread
from sqlite3 import connect, IntegrityError
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import cgi
from dataclasses import asdict

import requests

from broker.message import *
from api.manager import BacaApiManager
from api.server import BacaApiServer


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
        print(message)


def server_run(server_class=HTTPServer, handler_class=DummyBacaServer, port=8008):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)

    print('Starting httpd on port %d...' % port)
    th = Thread(target=httpd.serve_forever)
    th.start()
    return httpd, th


def dummy_check_submit(*args, **kwargs) -> None:
    pass


def dummy_result_parser(a: str, b: int, c: Path) -> BrokerToBaca:
    return BrokerToBaca(
        course_name=a,
        submit_id=b,
        tests=[Test('OK', 12.2, 10.0, 10000)]
    )


class BasicTests(ut.TestCase):

    DB_PATH = './test.db'

    def setUp(self) -> None:
        db = connect(self.DB_PATH)
        with open('../api/database.sql') as f:
            db.execute(f.read())
        db.close()
        self.manager = BacaApiManager(
            result_parser=dummy_result_parser,
            database=Path(self.DB_PATH),
            baca_api_url='http://127.0.0.1:8008/broker_api/'
        )
        self.server_baca, self.server_baca_thread = server_run(port=8008)
        self.server_broker = BacaApiServer(self.manager, server_port=9080)
        self.server_broker.start()

    def tearDown(self) -> None:
        self.server_baca.shutdown()
        self.server_baca.server_close()
        self.server_baca_thread.join()
        os.remove(self.DB_PATH)
        self.server_broker.stop()

    def test_insert(self):
        btb = BacaToBroker(
            course_name='Name',
            submit_id=1,
            package_path='package/test/path',
            solution_path='solution/test/path'
        )
        req = self.manager.insert(btb)
        con = sqlite3.connect(self.DB_PATH)
        cur = con.execute(f"SELECT * FROM baca_requests WHERE id = ?", (BacaApiManager._generate_submit_id('Name', 1),))
        tmp = cur.fetchone()
        self.assertTrue(tmp is not None)
        dummy_check_submit()
        self.manager.mark_and_send(req, Path('result/path'))
        cur = con.execute(f"SELECT * FROM baca_requests WHERE id = ?", (BacaApiManager._generate_submit_id('Name', 1),))
        tmp = cur.fetchone()
        con.close()
        self.assertTrue(tmp is None)

    def test_server(self):
        btb = BacaToBroker(
            course_name='Name',
            submit_id=1,
            package_path='package/test/path',
            solution_path='solution/test/path'
        )
        requests.post('http://127.0.0.1:9080/', json=asdict(btb))
        con = sqlite3.connect(self.DB_PATH)
        cur = con.execute(f"SELECT * FROM baca_requests WHERE id = ?", (BacaApiManager._generate_submit_id('Name', 1),))
        tmp = cur.fetchone()
        con.close()
        self.assertTrue(tmp is not None)

    def test_server_and_send(self):
        for i in range(1000):
            btb = BacaToBroker(
                course_name=f'Name',
                submit_id=i,
                package_path=f'package/test/path{i}',
                solution_path=f'solution/test/path{i}'
            )
            requests.post('http://127.0.0.1:9080/', json=asdict(btb))
        con = sqlite3.connect(self.DB_PATH)
        cur = con.execute(f"SELECT * FROM baca_requests")
        tmp = cur.fetchall()
        sub_list = list(sorted(map(lambda x: BacaApiManager._generate_submit_id('Name', x), range(1000))))
        self.assertEquals(sub_list, list(sorted(map(lambda x: x[0], tmp))))
        for i in range(1000):
            self.manager.mark_and_send(BacaApiManager._generate_submit_id('Name', i), '/path/x')
        cur = con.execute(f"SELECT * FROM baca_requests")
        tmp = cur.fetchall()
        con.close()
        self.assertEquals(tmp, [])

    def test_manager_database_errors(self):
        btb = BacaToBroker(
            course_name='Name',
            submit_id=1,
            package_path='package/test/path',
            solution_path='solution/test/path'
        )
        requests.post('http://127.0.0.1:9080/', json=asdict(btb))
        self.assertRaises(IntegrityError, self.manager.insert, btb)
        self.assertRaises(KeyError, self.manager.mark_and_send, 'non-exist:1', '/d/d/d')
        self.assertRaises(KeyError, self.manager._mark_checked, 'non-exist:2')
        self.assertRaises(KeyError, self.manager._send, 'non-exist:3', 'd/d/d')
