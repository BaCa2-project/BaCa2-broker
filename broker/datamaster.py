import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from enum import Enum
from pathlib import Path

from baca2PackageManager import Package
from baca2PackageManager.broker_communication import BrokerToBaca


class SetSubmitInterface(ABC):

    class SetState(Enum):
        INITIAL = 1
        SENDING_TO_KOLEJKA = 2
        AWAITING_KOLEJKA = 3
        DONE = 4

    def __init__(self,
                 master: 'DataMasterInterface',
                 task_submit: 'TaskSubmitInterface',
                 set_name: str):
        self.master = master
        self.task_submit = task_submit
        self.state: SetSubmitInterface.SetState = SetSubmitInterface.SetState.INITIAL
        self.set_name = set_name
        self.creation_date = datetime.now()
        self.mod_date = self.creation_date

    def change_state(self, new_state: SetState):
        self.mod_date = datetime.now()
        self.state = new_state

    @abstractmethod
    def set_result(self, result: BrokerToBaca):
        pass

    @abstractmethod
    def get_result(self) -> BrokerToBaca:
        pass

    @abstractmethod
    def set_status_code(self, status_code: str):
        pass

    @abstractmethod
    def get_status_code(self) -> str:
        pass


class SetSubmit(SetSubmitInterface):

    def __init__(self,
                 master: 'DataMasterInterface',
                 task_submit: 'TaskSubmit',
                 set_name: str):
        super().__init__(master, task_submit, set_name)
        self.result: Optional[BrokerToBaca] = None
        self.status_code: Optional[str] = None

    def set_result(self, result: BrokerToBaca):
        self.result = result

    def get_result(self) -> BrokerToBaca:
        if self.result is None:
            raise ValueError("No result available")
        return self.result

    def set_status_code(self, status_code: str):
        self.status_code = status_code

    def get_status_code(self) -> str:
        if self.status_code is None:
            raise ValueError("No status code available")
        return self.status_code


class TaskSubmitInterface(ABC):

    class TaskState(Enum):
        INITIAL = 1
        AWAITING_SETS = 2
        DONE = 3
        ERROR = -1

    def __init__(self,
                 master: 'DataMasterInterface',
                 task_submit_id: str,
                 package_path: Path,
                 commit_id: str,
                 submit_path: Path):
        self.master = master
        self.submit_id = task_submit_id
        self.state: TaskSubmitInterface.TaskState = TaskSubmitInterface.TaskState.INITIAL
        self.package_path = package_path
        self.commit_id = commit_id
        self.submit_path = submit_path
        self.creation_date = datetime.now()
        self.mod_date = self.creation_date

    def change_state(self, new_state: TaskState):
        self.mod_date = datetime.now()
        self.state = new_state

    @staticmethod
    @abstractmethod
    def make_set_submit_id(task_submit_id: str, set_name: str) -> str:
        pass

    @abstractmethod
    async def initialise(self):
        pass

    @abstractmethod
    def all_checked(self) -> bool:
        pass

    @property
    @abstractmethod
    def package(self) -> Package:
        pass

    @property
    @abstractmethod
    def set_submits(self) -> list[SetSubmitInterface]:
        pass

    @property
    @abstractmethod
    def results(self) -> list[BrokerToBaca]:
        pass


class TaskSubmit(TaskSubmitInterface):

    def __init__(self,
                 master: 'DataMasterInterface',
                 task_submit_id: str,
                 package_path: Path,
                 commit_id: str,
                 submit_path: Path):
        super().__init__(master, task_submit_id, package_path, commit_id, submit_path)
        self._package: Package = None
        self._sets: list[SetSubmitInterface] = None

    @staticmethod
    def make_set_submit_id(task_submit_id: str, set_name: str) -> str:
        return f"{task_submit_id}_{set_name}"

    async def initialise(self):
        if self._sets is not None:
            raise ValueError("Sets already filled")
        self._sets = []
        self._package = await asyncio.to_thread(Package, self.package_path, self.commit_id)
        for t_set in await asyncio.to_thread(self.package.sets):
            set_submit = self.master.new_set_submit(self, t_set['name'])
            self._sets.append(set_submit)

    def all_checked(self) -> bool:
        if self._sets is None:
            raise ValueError("Sets not filled")
        return all([s.state == SetSubmit.SetState.DONE for s in self.set_submits])

    @property
    def package(self) -> Package:
        if self._package is None:
            raise ValueError("Package not filled")
        return self._package

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


class DataMasterInterface(ABC):

    class DataMasterError(Exception):
        pass

    def __init__(self, task_submit_t: type[TaskSubmitInterface], set_submit_t: type[SetSubmitInterface]):
        self.task_submit_t = task_submit_t
        self.set_submit_t = set_submit_t

    @abstractmethod
    def new_set_submit(self, task_submit: 'TaskSubmitInterface', set_name: str) -> SetSubmitInterface:
        pass

    @abstractmethod
    def new_task_submit(self,
                        task_submit_id: str,
                        package_path: Path,
                        commit_id: str,
                        submit_path: Path) -> TaskSubmitInterface:
        pass

    @abstractmethod
    def delete_task_submit(self, task_submit: TaskSubmitInterface):
        pass

    @abstractmethod
    def get_set_submit(self, submit_id: str) -> SetSubmitInterface:
        pass

    @abstractmethod
    def get_task_submit(self, submit_id: str) -> TaskSubmitInterface:
        pass


class DataMaster(DataMasterInterface):

    def __init__(self, task_submit_t: type[TaskSubmitInterface], set_submit_t: type[SetSubmitInterface]):
        super().__init__(task_submit_t, set_submit_t)
        self.task_submits: dict[str, TaskSubmit] = {}
        self.set_submits: dict[str, SetSubmit] = {}

    def new_task_submit(self,
                        task_submit_id: str,
                        package_path: Path,
                        commit_id: str,
                        submit_path: Path) -> TaskSubmitInterface:
        if task_submit_id in self.task_submits:
            raise self.DataMasterError(f"Task submit {task_submit_id} already exists")
        task_submit = self.task_submit_t(self, task_submit_id, package_path, commit_id, submit_path)
        self.task_submits[task_submit_id] = task_submit
        return task_submit

    def new_set_submit(self, task_submit: 'TaskSubmitInterface', set_name: str, ) -> SetSubmitInterface:
        set_submit_id = task_submit.make_set_submit_id(task_submit.submit_id, set_name)
        if set_submit_id in self.set_submits:
            raise self.DataMasterError(f"Set submit {set_submit_id} already exists")
        set_submit = self.set_submit_t(self, task_submit, set_name)
        self.set_submits[set_submit_id] = set_submit
        return set_submit

    def delete_task_submit(self, task_submit: TaskSubmitInterface):
        if task_submit.submit_id not in self.task_submits:
            raise self.DataMasterError(f"Task submit {task_submit.submit_id} does not exist")
        for set_submit in task_submit.set_submits:
            self._delete_set_submit(set_submit)
        del self.task_submits[task_submit.submit_id]

    def _delete_set_submit(self, set_submit: SetSubmitInterface):
        set_id = set_submit.task_submit.make_set_submit_id(set_submit.task_submit.submit_id, set_submit.set_name)
        if set_id not in self.set_submits:
            raise self.DataMasterError(f"Set submit {set_id} does not exist")
        del self.set_submits[set_id]

    def get_set_submit(self, submit_id: str) -> SetSubmitInterface:
        if submit_id not in self.set_submits:
            raise self.DataMasterError(f"Set submit {submit_id} does not exist")
        return self.set_submits[submit_id]

    def get_task_submit(self, submit_id: str) -> TaskSubmitInterface:
        if submit_id not in self.task_submits:
            raise self.DataMasterError(f"Task submit {submit_id} does not exist")
        return self.task_submits[submit_id]
