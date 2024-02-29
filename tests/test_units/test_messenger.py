import asyncio
import logging
import unittest
from threading import Thread
from time import sleep

from fastapi import FastAPI, HTTPException
import uvicorn
from baca2PackageManager import Package
from baca2PackageManager.broker_communication import BrokerToBaca

from app.broker.messenger import BacaMessenger
from app.broker.datamaster import TaskSubmitInterface, SetSubmitInterface


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
    def results(self) -> dict[str, BrokerToBaca]:
        return {}

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

    def test_baca_send_exception(self):
        self.baca_messenger.baca_success_url = f"http://localhost:{self.TEST_PORT}/success_error"
        task_submit = MockTaskSubmit(master=None, task_submit_id="submit_id", package_path=None,
                                     commit_id="commit_id",
                                     submit_path=None)
        with self.assertRaises(Exception):
            asyncio.run(self.baca_messenger.send(task_submit))

    def test_baca_error(self):
        task_submit = MockTaskSubmit(master=None, task_submit_id="submit_id", package_path=None,
                                     commit_id="commit_id",
                                     submit_path=None)
        out = asyncio.run(self.baca_messenger.send_error(task_submit, "error"))
        self.assertTrue(out)


if __name__ == '__main__':
    unittest.main()
