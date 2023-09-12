from dataclasses import dataclass


@dataclass
class BacaToBroker:
    submit_id: str
    package_path: str
    commit_id: str
    submit_path: str


@dataclass
class TestResult:
    name: str
    status: str
    time_real: float
    time_cpu: float
    runtime_memory: int


@dataclass
class SetResult:
    name: str
    tests: dict[str, TestResult]


@dataclass
class BrokerToBaca:
    submit_id: str
    results: dict[str, SetResult]
