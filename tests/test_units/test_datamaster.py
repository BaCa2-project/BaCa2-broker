import asyncio
import os
import unittest
from datetime import timedelta
from pathlib import Path

from baca2PackageManager import Package
from baca2PackageManager.broker_communication import BrokerToBaca

from app.broker.datamaster import (DataMasterInterface, TaskSubmitInterface, SetSubmitInterface,
                                   DataMaster, TaskSubmit, SetSubmit, StateError)
from app.logger import LoggerManager


class DatamasterTest(unittest.TestCase):

    test_dir = Path(__file__).parent.parent

    class TaskSubmitMock(TaskSubmitInterface):
        @staticmethod
        def make_set_submit_id(task_submit_id: str, set_name: str) -> str:
            return f"{task_submit_id}_{set_name}"

        def __init__(self,
                     master: 'DataMasterInterface',
                     task_submit_id: str,
                     package_path: Path,
                     commit_id: str,
                     submit_path: Path):
            super().__init__(master, task_submit_id, package_path, commit_id, submit_path)
            self._sets: list[SetSubmitInterface] = None

        async def initialise(self):
            if self._sets is not None:
                raise ValueError("Sets already filled")
            self._sets = []
            for t_set in range(3):
                set_submit = self.master.new_set_submit(self, str(t_set))
                self._sets.append(set_submit)

        def all_checked(self) -> bool:
            if self._sets is None:
                raise ValueError("Sets not filled")
            return all([s.state == SetSubmit.SetState.DONE for s in self.set_submits])

        @property
        def package(self) -> Package:
            return None

        @property
        def set_submits(self) -> list[SetSubmitInterface]:
            if self._sets is None:
                raise ValueError("Sets not filled")
            return self._sets.copy()

        @property
        def results(self) -> list[BrokerToBaca]:
            if not self.all_checked():
                raise ValueError("Sets not filled")
            return [s.get_result() for s in self.set_submits]

    def setUp(self):
        self.logger_manager = LoggerManager('test', self.test_dir / 'test.log', 0)
        self.logger_manager.set_formatter('%(filename)s:%(lineno)d: %(message)s')
        self.logger_manager.start()
        self.logger = self.logger_manager.logger
        self.data_master = DataMaster(self.TaskSubmitMock, SetSubmit, self.logger)

    def tearDown(self):
        self.logger_manager.stop()
        with open(self.test_dir / 'test.log') as f:
            print(f.read())
        os.remove(self.test_dir / 'test.log')

    def test_new_task_submit(self):
        task_submit = self.data_master.new_task_submit("submit_id", Path("package_path"),
                                                       "commit_id", Path("submit_path"))
        asyncio.run(task_submit.initialise())
        self.assertIsInstance(task_submit, TaskSubmitInterface)
        self.assertEqual(len(self.data_master.task_submits), 1)
        self.assertEqual(len(task_submit.set_submits), 3)

        self.assertRaises(self.data_master.DataMasterError, self.data_master.new_task_submit, "submit_id",
                          Path("package_path"), "commit_id", Path("submit_path"))

    def test_get_set_submit(self):
        task_submit = self.data_master.new_task_submit("submit_id", Path("package_path"),
                                                       "commit_id", Path("submit_path"))
        asyncio.run(task_submit.initialise())
        set_submit = task_submit.set_submits[0]
        self.assertEqual(set_submit, self.data_master.get_set_submit(task_submit.make_set_submit_id(
            task_submit.submit_id,
            set_submit.set_name)))
        self.assertRaises(self.data_master.DataMasterError, self.data_master.get_set_submit, "nonexistent")

    def test_get_task_submit(self):
        task_submit = self.data_master.new_task_submit("submit_id", Path("package_path"),
                                                       "commit_id", Path("submit_path"))
        asyncio.run(task_submit.initialise())
        self.assertEqual(task_submit, self.data_master.get_task_submit("submit_id"))
        self.assertRaises(self.data_master.DataMasterError, self.data_master.get_task_submit, "nonexistent")

    def test_delete_task_submit(self):
        task_submit = self.data_master.new_task_submit("submit_id", Path("package_path"),
                                                       "commit_id", Path("submit_path"))
        asyncio.run(task_submit.initialise())
        self.data_master.delete_task_submit(task_submit)
        self.assertEqual(len(self.data_master.task_submits), 0)
        self.assertEqual(len(self.data_master.set_submits), 0)

    def test_start_daemons(self):
        async def inner():
            task_submit_new = self.data_master.new_task_submit("submit_id_new", Path("package_path"),
                                                               "commit_id", Path("submit_path"))
            await task_submit_new.initialise()
            task_submit_old = self.data_master.new_task_submit("submit_id_old", Path("package_path"),
                                                               "commit_id", Path("submit_path"))
            await task_submit_old.initialise()
            task_submit_old.mod_date += timedelta(minutes=61)

            task = asyncio.create_task(self.data_master.start_daemons(task_submit_timeout=timedelta(minutes=60),
                                                                      interval=0))

            await asyncio.sleep(0.1)
            self.assertEqual(task_submit_old.state, TaskSubmit.TaskState.ERROR)
            self.assertEqual(task_submit_new.state, TaskSubmit.TaskState.INITIAL)
            self.assertEqual(len(self.data_master.task_submits), 1)
            self.assertEqual(len(self.data_master.set_submits), 3)
            task.cancel()

        asyncio.run(inner())


class SubmitsTest(unittest.TestCase):

    test_dir = Path(__file__).absolute().parent.parent
    resource_dir = test_dir / 'resources'

    def setUp(self):
        self.logger_manager = LoggerManager('test', self.test_dir / 'test.log', 0)
        self.logger_manager.set_formatter('%(filename)s:%(lineno)d: %(message)s')
        self.logger_manager.start()
        self.logger = self.logger_manager.logger
        self.data_master = DataMaster(TaskSubmit, SetSubmit, self.logger)
        package_path = self.resource_dir / '1'
        submit_path = self.resource_dir / '1' / '1' / 'prog' / 'solution.cpp'
        self.task_submit = self.data_master.new_task_submit("submit_id", package_path,
                                                            "1", submit_path)

    def tearDown(self):
        self.logger_manager.stop()
        with open(self.test_dir / 'test.log') as f:
            print(f.read())
        os.remove(self.test_dir / 'test.log')

    def test_new_task_submit(self):
        asyncio.run(self.task_submit.initialise())
        self.assertIsInstance(self.task_submit, TaskSubmit)
        self.assertEqual(len(self.data_master.task_submits), 1)
        self.assertEqual(len(self.task_submit.set_submits), 3)

    def test_change_state_set_submit(self):
        asyncio.run(self.task_submit.initialise())
        set_submit = self.task_submit.set_submits[0]
        set_submit.change_state(SetSubmit.SetState.SENDING_TO_KOLEJKA, requires=SetSubmit.SetState.INITIAL)
        self.assertEqual(set_submit.state, SetSubmit.SetState.SENDING_TO_KOLEJKA)
        with self.assertRaises(StateError):
            set_submit.change_state(SetSubmit.SetState.DONE, requires=SetSubmit.SetState.INITIAL)
        set_submit.change_state(SetSubmit.SetState.DONE, requires=SetSubmit.SetState.SENDING_TO_KOLEJKA)
        self.assertEqual(set_submit.state, SetSubmit.SetState.DONE)
        set_submit.change_state(SetSubmit.SetState.INITIAL, None)
        self.assertEqual(set_submit.state, SetSubmit.SetState.INITIAL)
        set_submit.change_state(SetSubmit.SetState.SENDING_TO_KOLEJKA,
                                requires=[SetSubmit.SetState.INITIAL, SetSubmit.SetState.DONE])
        self.assertEqual(set_submit.state, SetSubmit.SetState.SENDING_TO_KOLEJKA)

    def test_change_state_task_submit(self):
        with self.assertRaises(StateError):
            self.task_submit.change_state(TaskSubmit.TaskState.DONE, requires=TaskSubmit.TaskState.AWAITING_SETS)
        self.task_submit.change_state(TaskSubmit.TaskState.AWAITING_SETS, requires=TaskSubmit.TaskState.INITIAL)
        self.assertEqual(self.task_submit.state, TaskSubmit.TaskState.AWAITING_SETS)
        self.task_submit.change_state(TaskSubmit.TaskState.DONE,
                                      requires=[TaskSubmit.TaskState.AWAITING_SETS, TaskSubmit.TaskState.ERROR])
        self.assertEqual(self.task_submit.state, TaskSubmit.TaskState.DONE)
        self.task_submit.change_state(TaskSubmit.TaskState.INITIAL, None)
        self.assertEqual(self.task_submit.state, TaskSubmit.TaskState.INITIAL)

    def test_set_results(self):
        asyncio.run(self.task_submit.initialise())
        set_submit = self.task_submit.set_submits[0]
        result = BrokerToBaca("submit_id", "hash", "package_path")
        self.assertRaises(ValueError, set_submit.get_result)
        set_submit.set_result(result)
        self.assertEqual(set_submit.get_result(), result)

    def test_set_status_code(self):
        asyncio.run(self.task_submit.initialise())
        set_submit = self.task_submit.set_submits[0]
        self.assertRaises(ValueError, set_submit.get_status_code)
        set_submit.set_status_code("200")
        self.assertEqual(set_submit.get_status_code(), "200")

    def test_get_results(self):
        asyncio.run(self.task_submit.initialise())
        self.assertRaises(ValueError, lambda: self.task_submit.results)
        result = BrokerToBaca("submit_id", "hash", "package_path")
        for set_submit in self.task_submit.set_submits:
            set_submit.change_state(SetSubmit.SetState.DONE, requires=None)
            set_submit.set_result(result)
        self.assertEqual(self.task_submit.results,
                         {set_submit.set_name: result for set_submit in self.task_submit.set_submits})


if __name__ == '__main__':
    unittest.main()
