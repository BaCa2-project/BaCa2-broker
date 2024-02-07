import subprocess
import sys
from abc import ABC, abstractmethod
from copy import deepcopy
import asyncio
from pathlib import Path

import requests
import yaml
from baca2PackageManager import Package
from baca2PackageManager.broker_communication import BrokerToBaca, make_hash, BrokerToBacaError, SetResult, TestResult

from .master import BrokerMaster
from .datamaster import TaskSubmit, SetSubmit
from .yaml_tags import get_loader


class KolejkaMessengerInterface(ABC):

    class KolejkaCommunicationFailed(Exception):
        pass

    def __init__(self, master: BrokerMaster):
        self.master = master

    @abstractmethod
    async def send(self, set_submit: SetSubmit) -> str:
        ...

    @abstractmethod
    async def get_results(self, set_submit: SetSubmit, result_code: str) -> SetResult:
        ...


class KolejkaMessenger(KolejkaMessengerInterface):

    def __init__(self, master: BrokerMaster, submits_dir: Path, build_namespace: str, kolejka_conf: Path):
        super().__init__(master)
        self.submits_dir = submits_dir  # SUBMITS_DIR
        self.build_namespace = build_namespace  # BUILD_NAMESPACE
        self.kolejka_conf = kolejka_conf  # KOLEJKA_CONF
        self.python_call: str = 'python3' if sys.platform.startswith('win') else 'py'

    def get_kolejka_client(self, package: Package) -> Path:
        return package.build_path(self.build_namespace) / 'common' / 'kolejka-client'

    def get_kolejka_judge(self, package: Package) -> Path:
        return package.build_path(self.build_namespace) / 'common' / 'kolejka-judge'

    def get_judge_py(self, package: Package) -> Path:
        return package.build_path(self.build_namespace) / 'common' / 'judge.py'

    @staticmethod
    def _translate_paths(*args):
        for arg in args:
            yield arg if isinstance(arg, str) else str(arg)

    @classmethod
    def kolejka_callback_url(cls, submit_id: str) -> str:
        return f'http://localhost:8000/kolejka/{submit_id}'  # TODO

    def _send_submit(self, set_submit: SetSubmit) -> str:
        task_submit = set_submit.task_submit

        task_dir = self.submits_dir / task_submit.submit_id / f'{set_submit.set_name}.task'
        set_id = f'{task_submit.submit_id}_{set_submit.set_name}'
        callback_url = self.kolejka_callback_url(set_id)

        cmd_judge = [self.python_call, self.get_kolejka_judge(task_submit.package),
                     'task',
                     '--callback', callback_url,
                     '--library-path', self.get_kolejka_judge(task_submit.package),
                     self.get_judge_py(task_submit.package),
                     set_submit.package.build_path(self.build_namespace) / set_submit.set_name / "tests.yaml",
                     task_submit.submit_path,
                     task_dir]
        cmd_client = [self.python_call, self.get_kolejka_client(task_submit.package),
                      '--config-file', self.kolejka_conf,
                      'task', 'put',
                      task_dir]

        # kolejka-client result get <result_dir>

        cmd_judge = list(self._translate_paths(*cmd_judge))
        cmd_client = list(self._translate_paths(*cmd_client))

        judge_status = subprocess.run(cmd_judge)
        if judge_status.returncode != 0:
            raise self.KolejkaCommunicationFailed('KOLEJKA judge failed to create task.')

        client_status = subprocess.run(cmd_client, capture_output=True)
        result_code = client_status.stdout.decode('utf-8').strip()

        if client_status.returncode != 0:
            raise self.KolejkaCommunicationFailed('KOLEJKA client failed to communicate with KOLEJKA server.')

        return result_code

    def _results_get(self, set_submit: SetSubmit, result_code) -> SetResult:
        result_dir = self.submits_dir / set_submit.task_submit.submit_id / f'{set_submit.set_name}.result'

        result_get = [self.python_call,
                      self.get_kolejka_client(set_submit.task_submit.package),
                      '--config-file', self.kolejka_conf,
                      'result', 'get',
                      result_code,
                      result_dir]

        result_get = list(self._translate_paths(*result_get))

        result_status = subprocess.run(result_get, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL)

        if result_status.returncode != 0:
            raise self.KolejkaCommunicationFailed('KOLEJKA client failed to get results.')

        return self._parse_results(set_submit, result_dir)

    @staticmethod
    def _parse_results(set_submit: SetSubmit, result_dir: Path) -> SetResult:
        results_yaml = result_dir / 'results' / 'results.yaml'
        with open(results_yaml) as f:
            content: dict = yaml.load(f, Loader=get_loader())
        tests = {}
        for key, val in content.items():
            satori = val['satori']
            tmp = TestResult(
                name=key,
                status=satori['status'],
                time_real=float(satori['execute_time_real'][:-1]),
                time_cpu=float(satori['execute_time_cpu'][:-1]),
                runtime_memory=int(satori['execute_memory'][:-1])
            )
            tests[key] = tmp
        return SetResult(name=set_submit.set_name, tests=tests)

    async def send(self, set_submit: SetSubmit) -> str:
        return await asyncio.to_thread(self._send_submit, set_submit)

    async def get_results(self, set_submit: SetSubmit, result_code: str) -> bool:
        return await asyncio.to_thread(self._results_get, set_submit, result_code)


class BacaMessengerInterface(ABC):
    def __init__(self, master):
        self.master = master

    @abstractmethod
    async def send(self, task_submit: TaskSubmit):
        ...

    @abstractmethod
    async def send_error(self, task_submit: TaskSubmit, error: Exception) -> bool:
        ...


class BacaMessenger(BacaMessengerInterface):

    def __init__(self, master: BrokerMaster, baca_url: str, password: str):
        super().__init__(master)
        self.baca_url = baca_url
        self.password = password

    async def send(self, task_submit) -> bool:
        return await asyncio.to_thread(self._send_to_baca,
                                       task_submit, self.baca_url, self.password)

    async def send_error(self, task_submit: TaskSubmit, error: Exception) -> bool:
        return await asyncio.to_thread(self._send_error_to_baca,
                                       task_submit, str(error), self.baca_url, self.password)

    @staticmethod
    def _send_to_baca(task_submit: TaskSubmit, baca_url: str, password: str):
        message = BrokerToBaca(
            pass_hash=make_hash(password, task_submit.submit_id),
            submit_id=task_submit.submit_id,
            results=deepcopy(task_submit.results),
        )
        with requests.Session() as s:
            r = s.post(url=baca_url, json=message.serialize())

        if r.status_code != 200:
            raise ConnectionError(f'Failed to send results to baCa2. Status code: {r.status_code}')

    @staticmethod
    def _send_error_to_baca(task_submit: TaskSubmit, error_msg: str, baca_url: str, password: str) -> bool:
        message = BrokerToBacaError(
            pass_hash=make_hash(password, task_submit.submit_id),
            submit_id=task_submit.submit_id,
            error=error_msg
        )
        s = requests.Session()
        try:
            r = s.post(url=baca_url, json=message.serialize())
        except (requests.exceptions.RequestException, requests.exceptions.ChunkedEncodingError):
            return False
        else:
            return r.status_code == 200
        finally:
            s.close()
