from abc import ABC, abstractmethod
from enum import Enum


class SetSubmit(ABC):

    class SetState(Enum):
        INITIAL = 1
        AWAITING_KOLEJKA = 2
        RECEIVED = 3
        DONE = 4

    def __init__(self, master: 'DataMasterInterface'):
        self.state: SetSubmit.SetState = SetSubmit.SetState.INITIAL
        self.result = None

    @abstractmethod
    def set_result(self, result):
        ...

    @abstractmethod
    def get_result(self):
        ...


class TaskSubmit(ABC):

    class TaskState(Enum):
        INITIAL = 1
        AWAITING_SETS = 2
        TO_BE_SENT = 3
        DONE = 4

    def __init__(self, master: 'DataMasterInterface'):
        self.state: TaskSubmit.TaskState = TaskSubmit.TaskState.INITIAL

    @abstractmethod
    def get_set_submits(self):
        ...


class DataMasterInterface(ABC):

    def __init__(self):
        ...

    @abstractmethod
    def register_set_submit(self, set_submit: SetSubmit):
        ...

    @abstractmethod
    def register_task_submit(self, task_submit: TaskSubmit):
        ...

    @abstractmethod
    def delete_task_submit(self, task_submit: TaskSubmit):
        ...

    @abstractmethod
    def get_set_submits(self, submit_id: str) -> SetSubmit:
        ...

    @abstractmethod
    def get_task_submits(self, submit_id: str) -> TaskSubmit:
        ...
