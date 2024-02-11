import asyncio
import unittest
from pathlib import Path

from baca2PackageManager import Package
from baca2PackageManager.broker_communication import BrokerToBaca

from broker.datamaster import (DataMasterInterface, TaskSubmitInterface, SetSubmitInterface,
                               DataMaster, TaskSubmit, SetSubmit)


class DatamasterTest(unittest.TestCase):

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
        self.data_master = DataMaster(self.TaskSubmitMock, SetSubmit)

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


class SubmitsTest(unittest.TestCase):

    test_dir = Path(__file__).absolute().parent

    def setUp(self):
        self.data_master = DataMaster(TaskSubmit, SetSubmit)
        package_path = self.test_dir / 'test_packages' / '1'
        submit_path = self.test_dir / 'test_packages' / '1' / '1' / 'prog' / 'solution.cpp'
        self.task_submit = self.data_master.new_task_submit("submit_id", package_path,
                                                            "1", submit_path)

    def test_new_task_submit(self):
        asyncio.run(self.task_submit.initialise())
        self.assertIsInstance(self.task_submit, TaskSubmit)
        self.assertEqual(len(self.data_master.task_submits), 1)
        self.assertEqual(len(self.task_submit.set_submits), 3)

    def test_change_state_task_submit(self):
        self.task_submit.change_state(TaskSubmit.TaskState.AWAITING_SETS)
        self.assertEqual(self.task_submit.state, TaskSubmit.TaskState.AWAITING_SETS)
        self.task_submit.change_state(TaskSubmit.TaskState.DONE)
        self.assertEqual(self.task_submit.state, TaskSubmit.TaskState.DONE)

    def test_change_state_set_submit(self):
        asyncio.run(self.task_submit.initialise())
        set_submit = self.task_submit.set_submits[0]

        async def run():
            t3 = asyncio.create_task(set_submit.change_state(SetSubmit.SetState.DONE, timeout=None))
            await asyncio.sleep(0)
            t2 = asyncio.create_task(set_submit.change_state(SetSubmit.SetState.AWAITING_KOLEJKA, timeout=None))
            await asyncio.sleep(0)
            t1 = asyncio.create_task(set_submit.change_state(SetSubmit.SetState.SENDING_TO_KOLEJKA))
            await asyncio.gather(t1, t2, t3)

        asyncio.run(run())
        self.assertEqual(set_submit.state, SetSubmit.SetState.DONE)

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
            asyncio.run(set_submit.change_state(SetSubmit.SetState.SENDING_TO_KOLEJKA, timeout=0))
            asyncio.run(set_submit.change_state(SetSubmit.SetState.AWAITING_KOLEJKA, timeout=0))
            set_submit.set_result(result)
            asyncio.run(set_submit.change_state(SetSubmit.SetState.DONE, timeout=0))
        self.assertEqual(self.task_submit.results, [result, result, result])


if __name__ == '__main__':
    unittest.main()
