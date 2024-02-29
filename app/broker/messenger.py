"""Module for communication with KOLEJKA and BaCa2 and package managing."""
import os
import shutil
import stat
import subprocess
import sys
from abc import ABC, abstractmethod
from copy import deepcopy
import asyncio
from datetime import datetime
from pathlib import Path
import logging

import requests
import yaml
import aiohttp
from baca2PackageManager import Package
from baca2PackageManager.broker_communication import BrokerToBaca, make_hash, BrokerToBacaError, SetResult, TestResult

from .builder import Builder
from .datamaster import TaskSubmitInterface, SetSubmitInterface
from .yaml_tags import get_loader


class KolejkaMessengerInterface(ABC):
    """Interface for KOLEJKA communication."""

    class KolejkaCommunicationError(Exception):
        pass

    @abstractmethod
    async def send(self, set_submit: SetSubmitInterface):
        """Sends set submit to KOLEJKA."""
        pass

    @abstractmethod
    async def get_results(self, set_submit: SetSubmitInterface):
        """Retrieves results of set submit from KOLEJKA."""
        pass


class KolejkaMessenger(KolejkaMessengerInterface):
    """Class for KOLEJKA communication for when ACTIVE_WAIT is disabled."""

    def __init__(self,
                 submits_dir: Path,
                 build_namespace: str,
                 kolejka_conf: Path,
                 kolejka_callback_url_prefix: str,
                 logger: logging.Logger):
        self.submits_dir = submits_dir
        self.build_namespace = build_namespace
        self.kolejka_conf = kolejka_conf
        self.python_call: str = 'py' if sys.platform.startswith('win') else 'python3'
        self.kolejka_callback_url_prefix = kolejka_callback_url_prefix
        self.logger = logger

    def get_kolejka_client(self, package: Package) -> Path:
        return package.build_path(self.build_namespace) / 'common' / 'kolejka-client'

    def get_kolejka_judge(self, package: Package) -> Path:
        return package.build_path(self.build_namespace) / 'common' / 'kolejka-judge'

    def get_judge_py(self, package: Package) -> Path:
        return package.build_path(self.build_namespace) / 'common' / 'judge.py'

    def kolejka_callback_url(self, submit_id: str) -> str:
        mid = '' if self.kolejka_callback_url_prefix.endswith('/') else '/'
        return self.kolejka_callback_url_prefix + mid + str(submit_id)

    async def send(self, set_submit: SetSubmitInterface):
        try:
            start = datetime.now()
            await self._send_inner(set_submit)
            self.logger.debug("Sending submit '%s' lasted %s", set_submit.submit_id, datetime.now() - start)
        except Exception as e:
            raise self.KolejkaCommunicationError("Cannot communicate with KOLEJKA.") from e

    async def _send_inner(self, set_submit: SetSubmitInterface):
        task_submit = set_submit.task_submit

        task_dir = self.submits_dir / task_submit.submit_id / f'{set_submit.set_name}.task'
        set_id = task_submit.make_set_submit_id(task_submit.submit_id, set_submit.set_name)
        callback_url = self.kolejka_callback_url(set_id)

        cmd_judge = [self.python_call,
                     self.get_kolejka_judge(task_submit.package),
                     'task',
                     '--callback', callback_url,
                     '--library-path', self.get_kolejka_judge(task_submit.package),
                     self.get_judge_py(task_submit.package),
                     task_submit.package.build_path(self.build_namespace) / set_submit.set_name / "tests.yaml",
                     task_submit.submit_path,
                     task_dir]

        judge_future = await asyncio.create_subprocess_shell(subprocess.list2cmdline(cmd_judge),
                                                             stderr=asyncio.subprocess.PIPE)
        _, stderr = await judge_future.communicate()

        if judge_future.returncode != 0:
            raise self.KolejkaCommunicationError(f'KOLEJKA judge failed to create task; stderr:\n{stderr.decode()}')

        cmd_client = [self.python_call,
                      self.get_kolejka_client(task_submit.package),
                      '--config-file', self.kolejka_conf,
                      'task', 'put',
                      task_dir]

        client_future = await asyncio.create_subprocess_shell(subprocess.list2cmdline(cmd_client),
                                                              stdout=asyncio.subprocess.PIPE,
                                                              stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await client_future.communicate()
        result_code = stdout.decode('utf-8').strip()

        if client_future.returncode != 0:
            raise self.KolejkaCommunicationError(f'KOLEJKA client failed to communicate with KOLEJKA server. '
                                                 f'stderr:\n{stderr.decode()}')

        set_submit.set_status_code(result_code)

    async def get_results(self, set_submit: SetSubmitInterface):
        try:
            start = datetime.now()
            results = await self._get_results_inner(set_submit, set_submit.get_status_code())
            set_submit.set_result(results)
            self.logger.debug("Retrieving results of submit '%s' lasted %s",
                              set_submit.submit_id, datetime.now() - start)
        except Exception as e:
            self.logger.error(str(e))
            raise self.KolejkaCommunicationError("Cannot communicate with KOLEJKA.") from e

    async def _get_results_inner(self, set_submit: SetSubmitInterface, result_code: str) -> SetResult:
        result_dir = self.submits_dir / set_submit.task_submit.submit_id / f'{set_submit.set_name}.result'

        result_get = [self.python_call,
                      self.get_kolejka_client(set_submit.task_submit.package),
                      '--config-file', self.kolejka_conf,
                      'result', 'get',
                      result_code,
                      result_dir]

        result_future = await asyncio.create_subprocess_shell(
            subprocess.list2cmdline(result_get),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await result_future.communicate()

        if result_future.returncode != 0:
            raise self.KolejkaCommunicationError(f'KOLEJKA client failed to get results; stderr:\n{stderr.decode()}')

        return self._parse_results(set_submit, result_dir)

    @staticmethod
    def _parse_results(set_submit: SetSubmitInterface, result_dir: Path) -> SetResult:
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


class KolejkaMessengerActiveWait(KolejkaMessenger):
    """Class for KOLEJKA communication for when ACTIVE_WAIT is enabled."""

    async def _send_inner(self, set_submit: SetSubmitInterface):
        task_submit = set_submit.task_submit

        task_dir = self.submits_dir / task_submit.submit_id / f'{set_submit.set_name}.task'
        set_id = task_submit.make_set_submit_id(task_submit.submit_id, set_submit.set_name)
        callback_url = self.kolejka_callback_url(set_id)

        cmd_judge = [self.python_call,
                     self.get_kolejka_judge(task_submit.package),
                     'task',
                     '--callback', callback_url,
                     '--library-path', self.get_kolejka_judge(task_submit.package),
                     self.get_judge_py(task_submit.package),
                     task_submit.package.build_path(self.build_namespace) / set_submit.set_name / "tests.yaml",
                     task_submit.submit_path,
                     task_dir]

        judge_future = await asyncio.create_subprocess_shell(subprocess.list2cmdline(cmd_judge),
                                                             stderr=asyncio.subprocess.PIPE)
        _, stderr = await judge_future.communicate()

        if judge_future.returncode != 0:
            raise self.KolejkaCommunicationError(f'KOLEJKA judge failed to create task; stderr:\n{stderr.decode()}')

        set_submit.set_result(await self.results_task(set_submit))

    async def get_results(self, set_submit: SetSubmitInterface):
        pass  # results are retrieved in send

    async def results_task(self, set_submit: SetSubmitInterface) -> SetResult:
        task_submit = set_submit.task_submit
        task_dir = self.submits_dir / task_submit.submit_id / f'{set_submit.set_name}.task'
        result_dir = self.submits_dir / set_submit.task_submit.submit_id / f'{set_submit.set_name}.result'

        cmd_client_active_wait = [
            self.python_call,
            self.get_kolejka_client(task_submit.package),
            '--config-file', self.kolejka_conf,
            'execute',
            task_dir,
            result_dir
        ]

        result_future = await asyncio.create_subprocess_shell(
            subprocess.list2cmdline(cmd_client_active_wait),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await result_future.communicate()

        if result_future.returncode != 0:
            raise self.KolejkaCommunicationError(f'KOLEJKA client failed to get results; stderr:\n{stderr.decode()}')

        results = self._parse_results(set_submit, result_dir)
        return results


class BacaMessengerInterface(ABC):
    """Interface for BaCa2 communication."""

    class BacaMessengerError(Exception):
        pass

    @abstractmethod
    async def send(self, task_submit: TaskSubmitInterface):
        """Sends task submit to BaCa2."""
        pass

    @abstractmethod
    async def send_error(self, task_submit: TaskSubmitInterface, error: Exception) -> bool:
        """Sends error message to BaCa2."""
        pass


class BacaMessenger(BacaMessengerInterface):

    def __init__(self, baca_success_url: str, baca_failure_url: str, password: str, logger: logging.Logger):
        self.baca_success_url = baca_success_url
        self.baca_failure_url = baca_failure_url
        self.password = password
        self.logger = logger

    async def send(self, task_submit) -> int:
        try:
            return await self._send_to_baca(task_submit, self.baca_success_url, self.password)
        except Exception as e:
            raise self.BacaMessengerError("Cannot communicate with baCa2.") from e

    async def send_error(self, task_submit: TaskSubmitInterface, error: Exception) -> bool:
        try:
            return await self._send_error_to_baca(task_submit, str(error), self.baca_failure_url, self.password)
        except aiohttp.ClientError:
            return False

    @staticmethod
    async def _send_to_baca(task_submit: TaskSubmitInterface, baca_url: str, password: str):
        message = BrokerToBaca(
            pass_hash=make_hash(password, task_submit.submit_id),
            submit_id=task_submit.submit_id,
            results=deepcopy(task_submit.results),
        )

        async with aiohttp.ClientSession() as session:
            async with session.post(url=baca_url, json=message.json()) as response:
                status_code = response.status

        if status_code != 200:
            raise ConnectionError(f'Failed to send results to baCa2. Status code: {status_code}')

        return status_code

    @staticmethod
    async def _send_error_to_baca(task_submit: TaskSubmitInterface,
                                  error_msg: str,
                                  baca_url: str,
                                  password: str) -> bool:
        message = BrokerToBacaError(
            pass_hash=make_hash(password, task_submit.submit_id),
            submit_id=task_submit.submit_id,
            error=error_msg
        )
        async with aiohttp.ClientSession() as session:
            async with session.post(url=baca_url, json=message.json()) as response:
                status_code = response.status

        return status_code == 200


class PackageManagerInterface(ABC):
    """Interface for package management."""

    def __init__(self, force_rebuild: bool):
        self.force_rebuild = force_rebuild

    @abstractmethod
    async def check_build(self, package: Package) -> bool:
        """Checks if package needs to be rebuilt."""
        pass

    @abstractmethod
    async def build_package(self, package: Package):
        """Builds package."""
        pass


class PackageManager(PackageManagerInterface):

    def __init__(self,
                 kolejka_src_dir: Path,
                 build_namespace: str,
                 force_rebuild: bool):
        super().__init__(force_rebuild)
        self.kolejka_src_dir = kolejka_src_dir
        self.build_namespace = build_namespace

    def refresh_kolejka_src(self, add_executable_attr: bool = True):  # TODO: change to async?
        if self.kolejka_src_dir.is_dir():
            shutil.rmtree(self.kolejka_src_dir)
        self.kolejka_src_dir.mkdir()

        kolejka_judge = requests.get('https://kolejka.matinf.uj.edu.pl/kolejka-judge').content
        kolejka_client = requests.get('https://kolejka.matinf.uj.edu.pl/kolejka-client').content

        kolejka_judge_path = self.kolejka_src_dir / 'kolejka-judge'
        kolejka_client_path = self.kolejka_src_dir / 'kolejka-client'

        with open(kolejka_judge_path, mode='wb') as judge:
            judge.write(kolejka_judge)
        with open(kolejka_client_path, mode='wb') as client:
            client.write(kolejka_client)

        if add_executable_attr:
            current_judge = os.stat(kolejka_judge_path)
            current_client = os.stat(kolejka_client_path)

            os.chmod(kolejka_judge_path, current_judge.st_mode | stat.S_IEXEC)
            os.chmod(kolejka_client_path, current_client.st_mode | stat.S_IEXEC)

    async def check_build(self, package: Package) -> bool:
        return await asyncio.to_thread(package.check_build, self.build_namespace)

    async def build_package(self, package: Package):
        if self.force_rebuild:
            await asyncio.to_thread(self.refresh_kolejka_src)

        build_pkg = Builder(package)
        await asyncio.to_thread(build_pkg.build)
