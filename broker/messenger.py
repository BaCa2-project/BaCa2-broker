import subprocess
import sys
from abc import ABC, abstractmethod
from copy import deepcopy
import asyncio
from pathlib import Path

import yaml
import aiohttp
from aiologger import Logger
from baca2PackageManager import Package
from baca2PackageManager.broker_communication import BrokerToBaca, make_hash, BrokerToBacaError, SetResult, TestResult

from .master import BrokerMaster
from .datamaster import TaskSubmit, SetSubmit
from .yaml_tags import get_loader


class KolejkaMessengerInterface(ABC):

    class KolejkaCommunicationError(Exception):
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

    def __init__(self,
                 master: BrokerMaster,
                 submits_dir: Path,
                 build_namespace: str,
                 kolejka_conf: Path,
                 kolejka_callback_url_prefix: str,
                 logger: Logger):
        super().__init__(master)
        self.submits_dir = submits_dir  # SUBMITS_DIR
        self.build_namespace = build_namespace  # BUILD_NAMESPACE
        self.kolejka_conf = kolejka_conf  # KOLEJKA_CONF
        self.python_call: str = 'python3' if sys.platform.startswith('win') else 'py'
        self.kolejka_callback_url_prefix = kolejka_callback_url_prefix
        self.logger = logger

    def get_kolejka_client(self, package: Package) -> Path:
        return package.build_path(self.build_namespace) / 'common' / 'kolejka-client'

    def get_kolejka_judge(self, package: Package) -> Path:
        return package.build_path(self.build_namespace) / 'common' / 'kolejka-judge'

    def get_judge_py(self, package: Package) -> Path:
        return package.build_path(self.build_namespace) / 'common' / 'judge.py'

    def kolejka_callback_url(self, submit_id: str) -> str:
        return self.kolejka_callback_url_prefix + str(submit_id)

    async def send(self, set_submit: SetSubmit) -> str:
        try:
            return await self._send_inner(set_submit)
        except Exception as e:
            self.logger.error(str(e))
            raise self.KolejkaCommunicationError("Cannot communicate with KOLEJKA.") from e

    async def _send_inner(self, set_submit: SetSubmit) -> str:
        task_submit = set_submit.task_submit

        task_dir = self.submits_dir / task_submit.submit_id / f'{set_submit.set_name}.task'
        set_id = task_submit.make_set_submit_id(task_submit.submit_id, set_submit.set_name)
        callback_url = self.kolejka_callback_url(set_id)

        cmd_judge = [self.get_kolejka_judge(task_submit.package),
                     'task',
                     '--callback', callback_url,
                     '--library-path', self.get_kolejka_judge(task_submit.package),
                     self.get_judge_py(task_submit.package),
                     task_submit.package.build_path(self.build_namespace) / set_submit.set_name / "tests.yaml",
                     task_submit.submit_path,
                     task_dir]
        cmd_judge = ' '.join(map(subprocess.list2cmdline, cmd_judge))

        judge_future = await asyncio.create_subprocess_shell(
            f"{self.python_call} {cmd_judge}",
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await judge_future.communicate()

        if judge_future.returncode != 0:
            raise self.KolejkaCommunicationError(f'KOLEJKA judge failed to create task; stderr: {stderr}')

        cmd_client = [self.get_kolejka_client(task_submit.package),
                      '--config-file', self.kolejka_conf,
                      'task', 'put',
                      task_dir]
        cmd_client = ' '.join(map(subprocess.list2cmdline, cmd_client))

        client_future = await asyncio.create_subprocess_shell(
            f"{self.python_call} {cmd_client}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await client_future.communicate()
        result_code = stdout.decode('utf-8').strip()

        if client_future.returncode != 0:
            raise self.KolejkaCommunicationError('KOLEJKA client failed to communicate with KOLEJKA server.')

        return result_code

    async def get_results(self, set_submit: SetSubmit, result_code: str) -> SetResult:
        try:
            return await self._get_results_inner(set_submit, result_code)
        except Exception as e:
            self.logger.error(str(e))
            raise self.KolejkaCommunicationError("Cannot communicate with KOLEJKA.") from e

    async def _get_results_inner(self, set_submit: SetSubmit, result_code: str) -> SetResult:
        result_dir = self.submits_dir / set_submit.task_submit.submit_id / f'{set_submit.set_name}.result'

        result_get = [self.python_call,
                      self.get_kolejka_client(set_submit.task_submit.package),
                      '--config-file', self.kolejka_conf,
                      'result', 'get',
                      result_code,
                      result_dir]
        result_get = ' '.join(map(subprocess.list2cmdline, result_get))

        result_future = await asyncio.create_subprocess_shell(
            f'{self.python_call} {result_get}',
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await result_future.wait()

        if result_future.returncode != 0:
            raise self.KolejkaCommunicationError('KOLEJKA client failed to get results.')

        return self._parse_results(set_submit, result_dir)

    @staticmethod
    def _parse_results(set_submit: SetSubmit, result_dir: Path) -> SetResult:  # TODO: change to async?
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


class BacaMessengerInterface(ABC):

    class BacaMessengerError(Exception):
        pass

    def __init__(self, master):
        self.master = master

    @abstractmethod
    async def send(self, task_submit: TaskSubmit):
        ...

    @abstractmethod
    async def send_error(self, task_submit: TaskSubmit, error: Exception) -> bool:
        ...


class BacaMessenger(BacaMessengerInterface):

    def __init__(self, master: BrokerMaster, baca_url: str, password: str, logger: Logger):
        super().__init__(master)
        self.baca_url = baca_url
        self.password = password
        self.logger = logger

    async def send(self, task_submit):
        try:
            return await self._send_to_baca(task_submit, self.baca_url, self.password)
        except Exception as e:
            self.logger.error(str(e))
            raise self.BacaMessengerError("Cannot communicate with baCa2.") from e

    async def send_error(self, task_submit: TaskSubmit, error: Exception) -> bool:
        try:
            return await self._send_error_to_baca(task_submit, str(error), self.baca_url, self.password)
        except Exception as e:
            self.logger.error(str(e))
            return False

    @staticmethod
    async def _send_to_baca(task_submit: TaskSubmit, baca_url: str, password: str):
        message = BrokerToBaca(
            pass_hash=make_hash(password, task_submit.submit_id),
            submit_id=task_submit.submit_id,
            results=deepcopy(task_submit.results),
        )

        async with aiohttp.ClientSession() as session:
            async with session.post(url=baca_url, json=message.serialize()) as response:
                status_code = response.status

        if status_code != 200:
            raise ConnectionError(f'Failed to send results to baCa2. Status code: {status_code}')

    @staticmethod
    async def _send_error_to_baca(task_submit: TaskSubmit, error_msg: str, baca_url: str, password: str) -> bool:
        message = BrokerToBacaError(
            pass_hash=make_hash(password, task_submit.submit_id),
            submit_id=task_submit.submit_id,
            error=error_msg
        )
        async with aiohttp.ClientSession() as session:
            async with session.post(url=baca_url, json=message.serialize()) as response:
                status_code = response.status

        return status_code == 200
