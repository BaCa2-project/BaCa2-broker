import asyncio
import os
import unittest
from datetime import timedelta
from pathlib import Path

from baca2PackageManager import Package
from baca2PackageManager.broker_communication import SetResult, BacaToBroker

import settings
from app.broker import BrokerMaster
from app.broker.datamaster import DataMaster, SetSubmit, TaskSubmit, SetSubmitInterface, TaskSubmitInterface
from app.broker.messenger import KolejkaMessengerInterface, BacaMessengerInterface, PackageManagerInterface
from app.handlers import PassiveHandler, ActiveHandler
from app.logger import LoggerManager


class MasterTest(unittest.TestCase):
    test_dir = Path(__file__).parent.parent
    resource_dir = test_dir / 'resources'

    class KolejkaMessengerMock(KolejkaMessengerInterface):

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.raise_exception = False

        async def get_results(self, set_submit: SetSubmitInterface) -> SetResult:
            await asyncio.sleep(0.01)
            if self.raise_exception:
                raise Exception
            return SetResult(name='x', tests=[])

        async def send(self, set_submit: SetSubmitInterface):
            await asyncio.sleep(0.01)
            if self.raise_exception:
                raise Exception
            set_submit.set_status_code('200')

    class BacaMessengerMock(BacaMessengerInterface):

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.raise_exception = False

        async def send_error(self, task_submit: TaskSubmitInterface, error: Exception) -> bool:
            await asyncio.sleep(0.01)

        async def send(self, task_submit: TaskSubmitInterface):
            await asyncio.sleep(0.01)
            if self.raise_exception:
                raise Exception

    class PackageManagerMock(PackageManagerInterface):

        async def check_build(self, package: Package) -> bool:
            await asyncio.sleep(0.01)
            return False

        async def build_package(self, package: TaskSubmitInterface):
            await asyncio.sleep(0.01)

    def setUp(self):
        self.package_path = self.resource_dir / '1'
        self.submit_path = self.resource_dir / '1' / '1' / 'prog' / 'solution.cpp'

        self.logger_manager = LoggerManager('test', self.test_dir / 'test.log', 0)
        self.logger_manager.set_formatter(settings.LOGGER_PROMPT)
        self.logger_manager.start()
        self.logger = self.logger_manager.logger
        self.data_master = DataMaster(TaskSubmit, SetSubmit, self.logger)
        self.kolejka_messenger = self.KolejkaMessengerMock()
        self.baca_messenger = self.BacaMessengerMock()
        self.package_manager = self.PackageManagerMock(force_rebuild=False)
        self.master = BrokerMaster(self.data_master,
                                   self.kolejka_messenger,
                                   self.baca_messenger,
                                   self.package_manager,
                                   self.logger)
        self.handlers = PassiveHandler(self.master, self.logger)

    def tearDown(self):
        self.logger_manager.stop()
        with open(self.test_dir / 'test.log') as f:
            print(f.read())
        os.remove(self.test_dir / 'test.log')

    def test_baca_send(self):
        btb = BacaToBroker(pass_hash='x',
                           submit_id='submit1',
                           package_path=self.package_path,
                           commit_id='1',
                           submit_path=self.submit_path)
        asyncio.run(self.handlers.handle_baca(btb))
        self.assertTrue('submit1' in self.data_master.task_submits)
        self.assertEqual(len(self.data_master.set_submits), 3)
        self.data_master.delete_task_submit(self.data_master.task_submits['submit1'])

    def test_kolejka_receive(self):
        btb = BacaToBroker(pass_hash='x',
                           submit_id='submit1',
                           package_path=self.package_path,
                           commit_id='1',
                           submit_path=self.submit_path)
        asyncio.run(self.handlers.handle_baca(btb))
        task_submit = self.data_master.task_submits['submit1']
        self.assertEqual(task_submit.state, TaskSubmit.TaskState.AWAITING_SETS)
        for set_submit in task_submit.set_submits:
            self.assertEqual(set_submit.get_status_code(), '200')
            set_id = task_submit.make_set_submit_id(task_submit.submit_id, set_submit.set_name)
            asyncio.run(self.handlers.handle_kolejka(set_id))
            self.assertEqual(set_submit.state, SetSubmit.SetState.DONE)
        tmp = task_submit.set_submits[0]
        set_id = task_submit.make_set_submit_id(task_submit.submit_id, tmp.set_name)
        self.assertEqual(task_submit.state, TaskSubmit.TaskState.DONE)
        self.assertTrue(task_submit.submit_id not in self.data_master.task_submits)

        asyncio.run(self.handlers.handle_baca(btb))
        task_submit = self.data_master.task_submits['submit1']
        set_submit = task_submit.set_submits[0]
        set_id = task_submit.make_set_submit_id(task_submit.submit_id, set_submit.set_name)

    def test_trash_submit(self):
        btb = BacaToBroker(pass_hash='x',
                           submit_id='submit1',
                           package_path=self.package_path,
                           commit_id='1',
                           submit_path=self.submit_path)
        asyncio.run(self.handlers.handle_baca(btb))
        task_submit = self.data_master.task_submits['submit1']
        self.assertEqual(task_submit.state, TaskSubmit.TaskState.AWAITING_SETS)
        for set_submit in task_submit.set_submits:
            self.assertEqual(set_submit.state, SetSubmit.SetState.AWAITING_KOLEJKA)

        asyncio.run(self.master.trash_task_submit(task_submit, Exception('test')))

        self.assertEqual(task_submit.state, TaskSubmit.TaskState.ERROR)
        for set_submit in task_submit.set_submits:
            self.assertEqual(set_submit.state, SetSubmit.SetState.ERROR)

        self.assertTrue('submit1' not in self.data_master.task_submits)
        self.assertTrue(len(self.data_master.set_submits) == 0)

    def test_process(self):
        class BacaMessengerMockInner(BacaMessengerInterface):

            def __init__(self, ut: unittest.TestCase, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.ut = ut
                self.processed = []

            async def send_error(self, task_submit: TaskSubmitInterface, error: Exception) -> bool:
                await asyncio.sleep(0.01)

            async def send(self, task_submit: TaskSubmitInterface):
                self.ut.assertTrue(task_submit.state == TaskSubmit.TaskState.DONE)
                for set_submit in task_submit.set_submits:
                    self.ut.assertTrue(set_submit.state == SetSubmit.SetState.DONE)
                    self.ut.assertEqual(set_submit.get_status_code(), '200')
                self.processed.append(task_submit.submit_id)
                await asyncio.sleep(0.01)

        class KolejkaMessengerMockInner(KolejkaMessengerInterface):

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.raise_exception = False
                self.master = None
                self.handlers = None
                self.tasks = set()

            async def get_results(self, set_submit: SetSubmitInterface) -> SetResult:
                await asyncio.sleep(0.01)
                if self.raise_exception:
                    raise Exception
                return SetResult(name='x', tests=[])

            async def send(self, set_submit: SetSubmitInterface):
                await asyncio.sleep(0.01)
                if self.raise_exception:
                    raise Exception
                set_submit.set_status_code('200')
                if self.handlers is not None:
                    task = asyncio.create_task(self.handlers.handle_kolejka(set_submit.submit_id))
                    self.tasks.add(task)

        kolejka_messenger = KolejkaMessengerMockInner()
        baca_messenger = BacaMessengerMockInner(self)
        master = BrokerMaster(self.data_master,
                              kolejka_messenger,
                              baca_messenger,
                              self.package_manager,
                              self.logger)
        master.kolejka_messenger.master = master
        handlers = PassiveHandler(master, self.logger)
        kolejka_messenger.handlers = handlers
        btb_list = [BacaToBroker(pass_hash='x',
                                 submit_id=f'submit{i}',
                                 package_path=self.package_path,
                                 commit_id='1',
                                 submit_path=self.submit_path) for i in range(100)]

        async def run():
            await asyncio.gather(*[handlers.handle_baca(btb) for btb in btb_list], return_exceptions=True)
            await asyncio.gather(*kolejka_messenger.tasks, return_exceptions=True)

        asyncio.run(run())
        self.assertTrue(len(self.data_master.task_submits) == 0)
        self.assertEqual(100, len(baca_messenger.processed))

    def test_start_daemons(self):
        async def inner():
            task_submit_new = self.data_master.new_task_submit("submit_id_new",
                                                               self.package_path,
                                                               "1",
                                                               self.submit_path)
            await task_submit_new.initialise()
            task_submit_old = self.data_master.new_task_submit("submit_id_old",
                                                               self.package_path,
                                                               "1",
                                                               self.submit_path)
            await task_submit_old.initialise()
            task_submit_old.mod_date += timedelta(minutes=61)

            task = asyncio.create_task(self.master.start_daemons(task_submit_timeout=timedelta(minutes=60),
                                                                 interval=0))

            await asyncio.sleep(0.1)
            self.assertEqual(task_submit_old.state, TaskSubmit.TaskState.ERROR)
            self.assertEqual(task_submit_new.state, TaskSubmit.TaskState.INITIAL)
            self.assertEqual(len(self.data_master.task_submits), 1)
            self.assertEqual(len(self.data_master.set_submits), 3)
            task.cancel()

        asyncio.run(inner())


class ActiveHandlerTest(unittest.TestCase):

    resource_dir = MasterTest.resource_dir
    test_dir = MasterTest.test_dir

    class KolejkaMessengerMock(KolejkaMessengerInterface):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.raise_exception = False
            self.processed: list[SetSubmitInterface] = []

        async def get_results(self, set_submit: SetSubmitInterface):
            pass

        async def send(self, set_submit: SetSubmitInterface):
            await asyncio.sleep(0.01)
            if self.raise_exception:
                raise Exception
            self.processed.append(set_submit)
            set_submit.set_result(SetResult(name='x', tests=[]))

    def setUp(self):
        self.package_path = self.resource_dir / '1'
        self.submit_path = self.resource_dir / '1' / '1' / 'prog' / 'solution.cpp'

        self.logger_manager = LoggerManager('test', self.test_dir / 'test.log', 0)
        self.logger_manager.set_formatter('%(filename)s:%(lineno)d: %(message)s')
        self.logger_manager.start()
        self.logger = self.logger_manager.logger
        self.data_master = DataMaster(TaskSubmit, SetSubmit, self.logger)
        self.kolejka_messenger = self.KolejkaMessengerMock()
        self.baca_messenger = MasterTest.BacaMessengerMock()
        self.package_manager = MasterTest.PackageManagerMock(force_rebuild=False)
        self.master = BrokerMaster(self.data_master,
                                   self.kolejka_messenger,
                                   self.baca_messenger,
                                   self.package_manager,
                                   self.logger)
        self.handlers = ActiveHandler(self.master, self.master.kolejka_messenger, self.logger)

    def tearDown(self):
        self.logger_manager.stop()
        with open(self.test_dir / 'test.log') as f:
            print(f.read())
        os.remove(self.test_dir / 'test.log')

    def test_handler(self):
        btb = BacaToBroker(pass_hash='x',
                           submit_id='submit1',
                           package_path=self.package_path,
                           commit_id='1',
                           submit_path=self.submit_path)
        asyncio.run(self.handlers.handle_baca(btb))
        set_submit = self.kolejka_messenger.processed[0]
        self.assertIsNotNone(set_submit.get_result())

    # def test_handler_many(self):
    #     async def tmp(t):
    #         await asyncio.gather(*t)
    #
    #     tasks = []
    #     for i in range(100):
    #         btb = BacaToBroker(pass_hash='x',
    #                            submit_id=f'submit{i}',
    #                            package_path=self.package_path,
    #                            commit_id='1',
    #                            submit_path=self.submit_path)
    #         tasks.append(self.handlers.handle_baca(btb))
    #     asyncio.run(tmp(tasks))
    #     # set_submit = self.kolejka_messenger.processed[0]
    #     # self.assertIsNotNone(set_submit.get_result())

    def test_handler_error(self):
        btb = BacaToBroker(pass_hash='x',
                           submit_id='submit1',
                           package_path=self.package_path,
                           commit_id='1',
                           submit_path=self.submit_path)
        self.kolejka_messenger.raise_exception = True
        asyncio.run(self.handlers.handle_baca(btb))
        self.assertTrue(len(self.kolejka_messenger.processed) == 0)


if __name__ == '__main__':
    unittest.main()
