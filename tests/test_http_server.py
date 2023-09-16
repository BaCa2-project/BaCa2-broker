"""Tests for broker.master.KolejkaCommunicationServer"""

import unittest as ut
from threading import Thread, Lock
from time import sleep
import requests

from broker.server import KolejkaCommunicationServer, BrokerIOServer, BrokerServerHandler


class DetailedTests(ut.TestCase):

    def setUp(self) -> None:
        self.manager = KolejkaCommunicationServer()

    def test_add_submit(self) -> None:
        def add_after_time(time: float, sub_id: str):
            sleep(time)
            self.manager.add_submit(sub_id)

        threads = []

        for i in range(1000):
            tmp = Thread(target=add_after_time, args=(10 - 0.01 * i, "%05d" % i))
            threads.append(tmp)
            tmp.start()

        for th in threads:
            th.join()

        self.assertEqual(len(self.manager), 1000)

    def test_release_submit(self) -> None:
        for i in range(1000):
            self.manager.add_submit("%05d" % i)

        def release_after_time(time: float, sub_id: str):
            sleep(time)
            self.manager.release_submit(sub_id)

        threads = []

        for i in range(1000):
            tmp = Thread(target=release_after_time, args=(10 - 0.01 * i, "%05d" % i))
            threads.append(tmp)
            tmp.start()

        for th in threads:
            th.join()

        for i in self.manager.submit_dict.values():
            self.assertFalse(i.locked())

    def test_await_submit(self) -> None:
        for i in range(1000):
            self.manager.add_submit("%05d" % i)

        def release_after_time(time: float, sub_id: str):
            sleep(time)
            self.manager.release_submit(sub_id)

        def await_after_time(time: float, sub_id: str):
            sleep(time)
            self.manager.await_submit(sub_id)

        await_threads = []

        for i in range(1000):
            tmp = Thread(target=await_after_time, args=(10 - 0.01 * i, "%05d" % i))
            await_threads.append(tmp)
            tmp.start()

        release_threads = []

        for i in range(1000):
            tmp = Thread(target=release_after_time, args=(10 - 0.01 * i, "%05d" % i))
            release_threads.append(tmp)
            tmp.start()

        for th in release_threads:
            th.join()

        for th in await_threads:
            th.join()

        self.assertEqual(len(self.manager), 0)

    def test_add_await_release_errors(self) -> None:
        submits = []

        # Create 1000 submits
        for i in range(1000):
            self.manager.add_submit("%05d" % i)
            submits.append("%05d" % i)
        self.assertTrue(len(self.manager.submit_dict) == 1000)
        # check if an exception is raised while trying to add duplicate submits
        for i in range(1000):
            self.assertRaises(ValueError, self.manager.add_submit, "%05d" % i)

        def impatient_student():
            # waits for half of the submits
            for j in reversed(submits[len(submits)//2:]):
                out = self.manager.await_submit(j, 5.5)
                self.assertTrue(out)
            for j in submits[:len(submits)//2]:
                self.manager.await_submit(j, 0.01)

        def kolejka():
            # release all submits
            for j in reversed(submits):
                self.manager.release_submit(j)
                sleep(0.01)
            # check if an exception is raised
            for j in submits:
                self.assertRaises((ValueError, KeyError), self.manager.release_submit, j)

        st = Thread(target=impatient_student)
        kl = Thread(target=kolejka)
        st.start()
        kl.start()
        st.join()
        kl.join()

        # at least half of the submits should be deleted
        print("test_add_await_release_errors: %d awaited for" % (1000 - len(self.manager)))
        self.assertTrue(len(self.manager) <= 1000//2)
        # all submits should be released
        for i in self.manager.submit_dict.values():
            self.assertFalse(i.locked())


class TestServerLoop(ut.TestCase):
    """General testing"""

    _log = True

    def setUp(self) -> None:
        self.manager = KolejkaCommunicationServer()
        self.server = BrokerIOServer(self.manager, None)
        self.host = self.server.ip
        self.port = self.server.port

    def tearDown(self) -> None:
        self.server.close_server()

    def check_http_response(self, *args, **kwargs) -> int:
        r = requests.post(url=f'http://{self.host}:{self.port}/{BrokerServerHandler.KOLEJKA_PATH}/{kwargs["sub_id"]}')
        return r.status_code


    def general(self, submit_number: int, wait_interval: float, log: bool = False) -> None:
        submits = []   # submits ready to be sent
        awaiting = []  # submits awaiting checking
        checked = []   # submits to be awaited for

        # data integrity locks for the above lists
        submits_lock = Lock()
        awaiting_lock = Lock()
        checked_lock = Lock()

        # generate submits
        for i in range(submit_number):
            submits.append('%010d' % i)

        def send_submits():
            """send submits to kolejka"""
            while True:
                submits_lock.acquire()
                if len(submits) == 0:
                    submits_lock.release()
                    break
                current = submits.pop(0)
                with awaiting_lock:
                    awaiting.append(current)
                self.manager.add_submit(current)
                if log:
                    print('%s sent' % current)
                submits_lock.release()
                sleep(wait_interval)

        def check_submits():
            """kolejka checks submits"""
            count = 0
            while count < submit_number:
                awaiting_lock.acquire()
                if len(awaiting) == 0:
                    awaiting_lock.release()
                    sleep(wait_interval)
                    continue
                cur = awaiting.pop(0)
                awaiting_lock.release()
                self.assertEqual(self.check_http_response(sub_id=cur), 200)
                if log:
                    print('%s checked' % cur)
                with checked_lock:
                    checked.append(cur)
                count += 1
                sleep(wait_interval)

        def wait_for_one(sub):
            self.manager.await_submit(sub)
            if log:
                print('%s awaited' % sub)

        def wait_for_submits():
            """baca broker waits for submits"""
            count = 0
            threads = []
            while count < submit_number:
                checked_lock.acquire()
                if len(checked) == 0:
                    checked_lock.release()
                    sleep(wait_interval)
                    continue
                cur = checked.pop(0)
                checked_lock.release()
                th = Thread(target=wait_for_one, args=[cur])
                threads.append(th)
                th.start()
                count += 1
                sleep(wait_interval)
            for thread in threads:
                thread.join()

        send = Thread(target=send_submits)
        check = Thread(target=check_submits)
        wait = Thread(target=wait_for_submits)
        send.start()
        check.start()
        wait.start()

        send.join()
        check.join()
        wait.join()

        # everything should be empty
        self.assertFalse(submits)
        self.assertFalse(awaiting)
        self.assertFalse(checked)
        self.assertFalse(self.manager.submit_dict)

    def test_general1(self) -> None:
        self.general(submit_number=100, wait_interval=0.05, log=self._log)

    def test_general2(self) -> None:
        self.general(submit_number=1000, wait_interval=0.005, log=self._log)

    def test_general3(self) -> None:
        self.general(submit_number=10000, wait_interval=0.0005, log=self._log)
