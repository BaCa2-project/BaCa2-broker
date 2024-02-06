from abc import ABC, abstractmethod
from typing import Optional
from enum import Enum
from pathlib import Path

from baca2PackageManager import Package
from baca2PackageManager.broker_communication import BrokerToBaca


class SetSubmitInterface(ABC):

    class SetState(Enum):
        INITIAL = 1
        AWAITING_KOLEJKA = 2
        DONE = 3

    def __init__(self,
                 master: 'DataMasterInterface',
                 task_submit: 'TaskSubmitInterface',
                 set_name: str,
                 package: Package,
                 submit_path: Path):
        self.master = master
        self.task_submit = task_submit
        self.state: SetSubmitInterface.SetState = SetSubmitInterface.SetState.INITIAL
        self.package = package
        self.set_name = set_name
        self.submit_path = submit_path

    @abstractmethod
    def set_result(self, result: BrokerToBaca):
        pass

    def change_state(self, new_state: SetState):
        self.master.handle_set_state_change(self, new_state)
        self.state = new_state

    @abstractmethod
    def get_result(self) -> BrokerToBaca:
        pass


class SetSubmit(SetSubmitInterface):

    def __init__(self,
                 master: 'DataMasterInterface',
                 task_submit: 'TaskSubmit',
                 set_name: str,
                 package: Package,
                 submit_path: Path):
        super().__init__(master, task_submit, set_name, package, submit_path)
        self.result: Optional[BrokerToBaca] = None

    def set_result(self, result: BrokerToBaca):
        self.result = result

    def change_state(self, new_state: SetSubmitInterface.SetState):
        self.master.handle_set_state_change(self, new_state)
        self.state = new_state

    def get_result(self) -> BrokerToBaca:
        if self.result is None:
            raise ValueError("No result available")
        return self.result


class TaskSubmitInterface(ABC):

    class TaskState(Enum):
        INITIAL = 1
        AWAITING_SETS = 2
        # TO_BE_SENT = 3
        DONE = 3

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

    @abstractmethod
    def initialise(self):
        pass

    def change_state(self, new_state: TaskState):
        self.master.handle_task_state_change(self, new_state)
        self.state = new_state

    @abstractmethod
    def all_checked(self) -> bool:
        pass

    @abstractmethod
    @property
    def set_submits(self) -> list[SetSubmitInterface]:
        pass


class TaskSubmit(TaskSubmitInterface):

    def __init__(self,
                 master: 'DataMasterInterface',
                 task_submit_id: str,
                 package_path: Path,
                 commit_id: str,
                 submit_path: Path):
        super().__init__(master, task_submit_id, package_path, commit_id, submit_path)
        self.package: Package = None
        self._sets: list[SetSubmitInterface] = None

    def initialise(self):
        if self._sets is not None:
            raise ValueError("Sets already filled")
        self._sets = []
        self.package = Package(self.package_path, self.commit_id)
        for t_set in self.package.sets():
            set_submit = self.master.register_set_submit(self, t_set['name'], self.package, self.submit_path)
            self._sets.append(set_submit)

    def change_state(self, new_state: TaskSubmitInterface.TaskState):
        self.master.handle_task_state_change(self, new_state)
        self.state = new_state

    def all_checked(self) -> bool:
        if self._sets is None:
            raise ValueError("Sets not filled")
        return all([s.state == SetSubmit.SetState.DONE for s in self._sets])

    @property
    def set_submits(self) -> list[SetSubmitInterface]:
        if self._sets is None:
            raise ValueError("Sets not filled")
        return self._sets.copy()


class DataMasterInterface(ABC):

    @abstractmethod
    def handle_task_state_change(self, task_submit: TaskSubmitInterface, new_state: TaskSubmitInterface.TaskState):
        ...

    @abstractmethod
    def handle_set_state_change(self, set_submit: SetSubmitInterface, new_state: SetSubmitInterface.SetState):
        ...

    @abstractmethod
    def register_set_submit(self,
                            task_submit: 'TaskSubmitInterface',
                            set_name: str,
                            package: Package,
                            submit_path: Path) -> SetSubmitInterface:
        ...

    @abstractmethod
    def new_task_submit(self,
                        task_submit_id: str,
                        package_path: Path,
                        commit_id: str,
                        submit_path: Path) -> TaskSubmitInterface:
        ...

    @abstractmethod
    def delete_task_submit(self, task_submit: TaskSubmitInterface):
        ...

    @abstractmethod
    def get_set_submit(self, submit_id: str) -> SetSubmitInterface:
        ...

    @abstractmethod
    def get_task_submit(self, submit_id: str) -> TaskSubmitInterface:
        ...


class DataMaster(DataMasterInterface):
    ...
