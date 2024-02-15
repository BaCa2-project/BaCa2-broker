import asyncio
import logging
import unittest
from pathlib import Path

from baca2PackageManager import Package
from baca2PackageManager.broker_communication import SetResult, BacaToBroker

from app.broker import BrokerMaster
from app.broker.datamaster import DataMaster, SetSubmit, TaskSubmit, SetSubmitInterface, TaskSubmitInterface
from app.broker.messenger import KolejkaMessengerInterface, BacaMessengerInterface, PackageManagerInterface


class MaterTest(unittest.TestCase):
    test_dir = Path(__file__).parent.parent

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
        self.package_path = self.test_dir / 'resources' / '1'
        self.submit_path = self.test_dir / 'resources' / '1' / '1' / 'prog' / 'solution.cpp'

        self.logger = logging.Logger('test')
        x = logging.Formatter('%(filename)s:%(lineno)d: %(message)s')
        self.logger.addHandler(logging.StreamHandler())
        self.logger.handlers[0].setFormatter(x)
        self.data_master = DataMaster(TaskSubmit, SetSubmit, self.logger)
        self.kolejka_messenger = self.KolejkaMessengerMock()
        self.baca_messenger = self.BacaMessengerMock()
        self.package_manager = self.PackageManagerMock(force_rebuild=False)
        self.master = BrokerMaster(self.data_master,
                                   self.kolejka_messenger,
                                   self.baca_messenger,
                                   self.package_manager,
                                   self.logger)

    def test_baca_send(self):
        btb = BacaToBroker(pass_hash='x',
                           submit_id='submit1',
                           package_path=self.package_path,
                           commit_id='1',
                           submit_path=self.submit_path)
        asyncio.run(self.master.handle_baca(btb))
        self.assertTrue('submit1' in self.data_master.task_submits)
        self.assertEqual(len(self.data_master.set_submits), 3)
        self.assertRaises(Exception, asyncio.run, self.master.handle_baca(btb))
        self.data_master.delete_task_submit(self.data_master.task_submits['submit1'])

        self.kolejka_messenger.raise_exception = True
        self.assertRaises(Exception, asyncio.run, self.master.handle_baca(btb))
        self.assertFalse('submit1' in self.data_master.task_submits)

    def test_kolejka_receive(self):
        btb = BacaToBroker(pass_hash='x',
                           submit_id='submit1',
                           package_path=self.package_path,
                           commit_id='1',
                           submit_path=self.submit_path)
        asyncio.run(self.master.handle_baca(btb))
        task_submit = self.data_master.task_submits['submit1']
        self.assertEqual(task_submit.state, TaskSubmit.TaskState.AWAITING_SETS)
        for set_submit in task_submit.set_submits:
            self.assertEqual(set_submit.get_status_code(), '200')
            set_id = task_submit.make_set_submit_id(task_submit.submit_id, set_submit.set_name)
            asyncio.run(self.master.handle_kolejka(set_id))
            self.assertEqual(set_submit.state, SetSubmit.SetState.DONE)
        tmp = task_submit.set_submits[0]
        set_id = task_submit.make_set_submit_id(task_submit.submit_id, tmp.set_name)
        self.assertRaises(self.data_master.DataMasterError, asyncio.run, self.master.handle_kolejka(set_id))
        self.assertEqual(task_submit.state, TaskSubmit.TaskState.DONE)
        self.assertTrue(task_submit.submit_id not in self.data_master.task_submits)

        asyncio.run(self.master.handle_baca(btb))
        task_submit = self.data_master.task_submits['submit1']
        set_submit = task_submit.set_submits[0]
        set_id = task_submit.make_set_submit_id(task_submit.submit_id, set_submit.set_name)
        self.kolejka_messenger.raise_exception = True
        self.assertRaises(Exception, asyncio.run, self.master.handle_kolejka(set_id))
        self.assertFalse('submit1' in self.data_master.task_submits)

    def test_trash_submit(self):
        btb = BacaToBroker(pass_hash='x',
                           submit_id='submit1',
                           package_path=self.package_path,
                           commit_id='1',
                           submit_path=self.submit_path)
        asyncio.run(self.master.handle_baca(btb))
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
                if self.master:
                    task = asyncio.create_task(self.master.handle_kolejka(set_submit.submit_id))
                    self.tasks.add(task)

        kolejka_messenger = KolejkaMessengerMockInner()
        baca_messenger = BacaMessengerMockInner(self)
        master = BrokerMaster(self.data_master,
                              kolejka_messenger,
                              baca_messenger,
                              self.package_manager,
                              self.logger)
        master.kolejka_messenger.master = master
        btb_list = [BacaToBroker(pass_hash='x',
                                 submit_id=f'submit{i}',
                                 package_path=self.package_path,
                                 commit_id='1',
                                 submit_path=self.submit_path) for i in range(100)]

        async def run():
            await asyncio.gather(*[master.handle_baca(btb) for btb in btb_list], return_exceptions=True)
            await asyncio.gather(*kolejka_messenger.tasks, return_exceptions=True)

        asyncio.run(run())
        print(self.data_master.task_submits)
        self.assertTrue(len(self.data_master.task_submits) == 0)
        self.assertEqual(100, len(baca_messenger.processed))


if __name__ == '__main__':
    unittest.main()
