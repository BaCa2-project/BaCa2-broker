import cgi
import os
import shutil
import unittest as ut
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from time import sleep

from baca2PackageManager import *

from broker.submit import SubmitState, TaskSubmit
from broker.master import BrokerMaster
from settings import BASE_DIR, BACA_PASSWORD, APP_SETTINGS

set_base_dir(BASE_DIR / 'tests' / 'test_packages')
add_supported_extensions('cpp')


class DummySubmit(TaskSubmit):

    def process(self):
        tmp = self._send_to_baca("http://127.0.0.1:8000/broker_api/results", BACA_PASSWORD)
        assert tmp is True
        self._change_state(SubmitState.DONE)


class DummyErrorSubmit(TaskSubmit):

    def process(self):
        raise self.JudgingError("Error")

    def _send_error_to_baca(self, baca_url: str, password: str, error_msg: str) -> bool:
        tmp = super()._send_error_to_baca(baca_url, password, error_msg)
        assert tmp is True


class DummyMaster(BrokerMaster):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.submit_type = None

    def set_submit_type(self, submit_type: type[TaskSubmit]):
        self.submit_type = submit_type

    def new_submit(self,
                   submit_id: str,
                   package_path: Path,
                   commit_id: str,
                   submit_path: Path):
        try:
            submit = self.submit_type(self,
                                      submit_id,
                                      package_path,
                                      commit_id,
                                      submit_path,
                                      force_rebuild=APP_SETTINGS['force_rebuild'],
                                      verbose=APP_SETTINGS['verbose'])
        except TaskSubmit.JudgingError:
            return
        self.submits[submit_id] = submit
        submit.start()


class DummyBacaServer(BaseHTTPRequestHandler):

    def _set_headers(self, resp: int):
        self.send_response(resp)
        self.send_header('Content-type', 'application/json')
        self.send_header('Content-Encoding', 'UTF-8')
        self.end_headers()

    def do_POST(self):
        c_type, pdict = cgi.parse_header(self.headers.get('content-type'))

        if c_type != 'application/json':
            self.send_response(400)
            self.end_headers()
            return

        if self.path == '/broker_api/results':
            self._set_headers(200)
        elif self.path == '/broker_api/error':
            self._set_headers(200)
        else:
            self._set_headers(404)

    @classmethod
    def server_run(cls, port=8000):
        server_address = ('127.0.0.1', port)
        httpd = ThreadingHTTPServer(server_address, cls)

        th = Thread(target=httpd.serve_forever)
        th.start()
        return httpd, th


class BasicTests(ut.TestCase):
    test_dir = Path(__file__).absolute().parent

    def setUp(self) -> None:
        if os.path.exists(self.test_dir / 'test.db'):
            os.remove(self.test_dir / 'test.db')
        if os.path.exists(self.test_dir / 'tmp_built'):
            shutil.rmtree(self.test_dir / 'tmp_built')
        self.server, self.s_thread = DummyBacaServer.server_run()
        os.mkdir(self.test_dir / 'tmp_built')
        self.master = DummyMaster(
            self.test_dir / 'test.db',
            self.test_dir / 'tmp_built',
            delete_records=False)
        self.master.start()
        self.master.set_submit_type(DummySubmit)
        os.system('sqlite3 {} <{}'.format(self.test_dir / 'test.db', self.test_dir.parent / 'db' / 'creator.sql'))

    def tearDown(self) -> None:
        self.master.stop()
        self.server.shutdown()
        self.server.server_close()
        self.s_thread.join()
        os.remove(self.test_dir / 'test.db')
        shutil.rmtree(self.test_dir / 'tmp_built')

    def test_one_submit(self):
        # self.master.set_submit_type(TaskSubmit)
        self.master.new_submit('1',
                               self.test_dir / 'test_packages' / '1',
                               '1',
                               submit_path=self.test_dir / 'test_packages' / '1' / '1' / 'prog' / 'solution.cpp')
        submit = self.master.submits['1']
        submit.join()
        self.assertEqual(SubmitState.DONE, submit.status)

    def test_many_submits(self):
        NUM = 100
        for i in range(NUM):
            self.master.new_submit(str(i),
                                   self.test_dir / 'test_packages' / '1',
                                   '1',
                                   submit_path=self.test_dir / 'test_packages' / '1' / '1' / 'prog' / 'solution.cpp')
            sleep(0.01)
        done = 0
        for i in range(NUM):
            submit = self.master.submits[str(i)]
            submit.join()
            done += 1 if submit.status == SubmitState.DONE else 0
        self.assertTrue(done >= 0.95 * NUM)

    def test_many_same_submits(self):
        class DummySubmit2(TaskSubmit):
            def process(self):
                sleep(1)
                self._change_state(SubmitState.SENDING)

        self.master.set_submit_type(DummySubmit2)

        NUM = 30
        for i in range(NUM):
            self.master.new_submit('1',
                                   self.test_dir / 'test_packages' / '1',
                                   '1',
                                   submit_path=self.test_dir / 'test_packages' / '1' / '1' / 'prog' / 'solution.cpp')
            sleep(0.01)
        tmp = self.master.connection.select('SELECT * FROM submit_records WHERE id = ?', 'all', '1')
        self.assertEqual(1, len(tmp))

    def test_error_submit(self):

        self.master.set_submit_type(DummyErrorSubmit)

        self.master.new_submit('1',
                               self.test_dir / 'test_packages' / '1',
                               '1',
                               submit_path=self.test_dir / 'test_packages' / '1' / '1' / 'prog' / 'solution.cpp')
        submit = self.master.submits['1']
        submit.join()
        self.assertEqual(SubmitState.ERROR, submit.status)

        class DummyTotalFailureSubmit(TaskSubmit):
            def process(self):
                raise self.BaCa2CommunicationError

            def _send_error_to_baca(self, baca_url: str, password: str, error_msg: str) -> bool:
                assert False  # Brah, you shouldn't be here

        # The following lines test if a message is sent when BaCa2CommunicationError is raised (it shouldn't be sent)
        self.master.set_submit_type(DummyTotalFailureSubmit)
        self.master.new_submit('2',
                               self.test_dir / 'test_packages' / '1',
                               '1',
                               submit_path=self.test_dir / 'test_packages' / '1' / '1' / 'prog' / 'solution.cpp')
        submit = self.master.submits['2']
        submit.join()

    def test_resend_error_submit(self):

        self.master.set_submit_type(DummyErrorSubmit)

        self.master.new_submit('1',
                               self.test_dir / 'test_packages' / '1',
                               '1',
                               submit_path=self.test_dir / 'test_packages' / '1' / '1' / 'prog' / 'solution.cpp')
        submit = self.master.submits['1']
        submit.join()
        self.assertEqual(SubmitState.ERROR, submit.status)

        self.master.set_submit_type(DummySubmit)
        self.master.new_submit('1',
                               self.test_dir / 'test_packages' / '1',
                               '1',
                               submit_path=self.test_dir / 'test_packages' / '1' / '1' / 'prog' / 'solution.cpp')
        submit = self.master.submits['1']
        submit.join()
        self.assertEqual(SubmitState.DONE, submit.status)
