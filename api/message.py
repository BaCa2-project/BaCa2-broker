
from dataclasses import dataclass


@dataclass
class BacaToBroker:
    course_name: str
    submit_id: int
    package_path: str
    solution_path: str


@dataclass
class BrokerToBaca:
    course_name: str
    submit_id: int
    status: str
    time_real: float
    time_cpu: float
    runtime_memory: int
