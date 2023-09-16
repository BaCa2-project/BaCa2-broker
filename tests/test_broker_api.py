import sqlite3
import unittest as ut
import os
from pathlib import Path
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import cgi
from dataclasses import asdict

import requests


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


class BasicTests(ut.TestCase):
    ...
