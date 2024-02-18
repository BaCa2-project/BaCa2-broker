import shutil

import asyncio
import logging
import unittest
from pathlib import Path
from threading import Thread
from time import sleep

from fastapi import FastAPI, HTTPException
import uvicorn
from baca2PackageManager import Package
from baca2PackageManager.broker_communication import BrokerToBaca
from settings import SUBMITS_DIR, BUILD_NAMESPACE, KOLEJKA_CONF, KOLEJKA_SRC_DIR

from app.broker.messenger import BacaMessenger, KolejkaMessenger, PackageManager
from app.broker.datamaster import TaskSubmitInterface, SetSubmitInterface, TaskSubmit, DataMaster, \
    SetSubmit

app = FastAPI()


@app.post("/success")
async def success():
    return {"message": "Success"}


@app.post("/success_error")
async def success_error():
    raise HTTPException(status_code=500, detail="Internal Server Error")


@app.post("/failure")
async def failure():
    return {"message": "Failure"}


class MockTaskSubmit(TaskSubmitInterface):

    @property
    def results(self) -> list[BrokerToBaca]:
        return []

    @property
    def set_submits(self) -> list[SetSubmitInterface]:
        return []

    @property
    def package(self) -> Package:
        return None

    def all_checked(self) -> bool:
        return True

    async def initialise(self):
        pass

    @staticmethod
    def make_set_submit_id(task_submit_id: str, set_name: str) -> str:
        return f'{task_submit_id}_{set_name}'


class BacaMessengerTest(unittest.TestCase):
    server_thread = None
    TEST_PORT = 9432

    @classmethod
    def setUpClass(cls):
        import settings

        cls.server_thread = Thread(target=uvicorn.run,
                                   args=(app,),
                                   kwargs={"host": "localhost", "port": cls.TEST_PORT},
                                   daemon=True)
        cls.server_thread.start()
        sleep(0.2)

    def setUp(self):
        self.logger = logging.Logger('test')
        self.logger.addHandler(logging.StreamHandler())
        self.baca_messenger = BacaMessenger(
            baca_success_url=f"http://localhost:{self.TEST_PORT}/success",
            baca_failure_url=f"http://localhost:{self.TEST_PORT}/failure",
            password="password",
            logger=self.logger
        )

    def test_baca_send(self):
        task_submit = MockTaskSubmit(master=None, task_submit_id="submit_id", package_path=None,
                                     commit_id="commit_id",
                                     submit_path=None)
        status_code = asyncio.run(self.baca_messenger.send(task_submit))
        self.assertEqual(200, status_code)

    def test_baca_send_error(self):
        self.baca_messenger.baca_success_url = f"http://localhost:{self.TEST_PORT}/success_error"
        task_submit = MockTaskSubmit(master=None, task_submit_id="submit_id", package_path=None,
                                     commit_id="commit_id",
                                     submit_path=None)
        with self.assertRaises(Exception):
            asyncio.run(self.baca_messenger.send(task_submit))


class KolejkaMessengerTest(unittest.TestCase):
    test_dir = Path(__file__).parent.parent

    def setUp(self):
        self.logger = logging.Logger('test')
        self.logger.addHandler(logging.StreamHandler())
        self.data_master = DataMaster(TaskSubmit, SetSubmit, self.logger)
        self.package_path = self.test_dir / 'resources' / '1'
        self.submit_path = self.test_dir / 'resources' / '1' / '1' / 'prog' / 'solution.cpp'
        self.kolejka_messanger = KolejkaMessenger(
            submits_dir=SUBMITS_DIR,
            build_namespace=BUILD_NAMESPACE,
            kolejka_conf=KOLEJKA_CONF,
            kolejka_callback_url_prefix='http://127.0.0.1/',
            logger=self.logger
        )
        self.package_manager = PackageManager(
            kolejka_src_dir=KOLEJKA_SRC_DIR,
            build_namespace=BUILD_NAMESPACE,
            force_rebuild=False,
        )
        self.package_manager.refresh_kolejka_src()

        shutil.rmtree(SUBMITS_DIR / 'submit_id', ignore_errors=True)
        shutil.rmtree(SUBMITS_DIR / '1', ignore_errors=True)

    def test_01_send_kolejka(self):  # TODO finish this test
        task_submit = self.data_master.new_task_submit(task_submit_id="1",
                                                       package_path=self.package_path,
                                                       commit_id="1",
                                                       submit_path=self.submit_path)
        asyncio.run(task_submit.initialise())
        asyncio.run(self.package_manager.build_package(task_submit.package))
        set_submit = task_submit.set_submits[0]
        asyncio.run(self.kolejka_messanger.send(set_submit))
        print(set_submit.get_status_code())
        self.assertIsNotNone(set_submit.get_status_code())
        self.assertNotEqual('', set_submit.get_status_code())

    def test_02_kolejka_receive(self):  # TODO: finish this test
        task_submit = self.data_master.new_task_submit(task_submit_id="submit_id",
                                                       package_path=self.package_path,
                                                       commit_id="1",
                                                       submit_path=self.submit_path)
        asyncio.run(task_submit.initialise())
        asyncio.run(self.package_manager.build_package(task_submit.package))
        set_submit = task_submit.set_submits[0]
        asyncio.run(self.kolejka_messanger.send(set_submit))
        sleep(20)
        asyncio.run(self.kolejka_messanger.get_results(set_submit))
        print(f'{set_submit.get_result()=}')


if __name__ == '__main__':
    unittest.main()
