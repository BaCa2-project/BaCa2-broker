from __future__ import annotations

import shutil
import subprocess
import sys
from threading import Thread, Lock
from pathlib import Path
from enum import Enum
from datetime import datetime
from time import sleep
from copy import deepcopy

import requests
import yaml
from baca2PackageManager import Package
from baca2PackageManager.broker_communication import *
from .builder import Builder
from settings import BUILD_NAMESPACE, KOLEJKA_CONF, BACA_PASSWORD, BACA_URL

from typing import TYPE_CHECKING

from .yaml_tags import get_loader

if TYPE_CHECKING:
    from .master import BrokerMaster


def _translate_paths(*args):
    for arg in args:
        if isinstance(arg, Path):
            yield str(arg)
        else:
            yield arg


class SubmitState(Enum):
    """
    Indicates the current state of submit.
    """
    #: Ending state if error occurred.
    ERROR = 500
    #: Submit canceled because of incompatible package structure.  
    CANCELED = 400
    #: Creating submit object.
    ADOPTING = 0
    #: Build and check waits for free threads
    AWAITING_PREPROC = 1
    #: If package is not built yet - package is built to KOLEJKA standard.
    BUILDING = 2
    #: If package is already built - check existence of crucial files.
    CHECKING = 3
    #: Transferring package with submit to KOLEJKA.
    SENDING = 4
    #: Waiting for callback from KOLEJKA.  
    AWAITING_JUDGE = 5
    #: Callback registered - results are being saved to db. 
    SAVING = 6
    #: Judging process ended successfully.
    DONE = 200
    #: Submit results successfully sent to BaCa2
    RETURNED = 201


class TaskSubmit(Thread):
    TIMEOUT_STEP = 3

    def __init__(self,
                 master: BrokerMaster,
                 submit_id: str,
                 package_path: Path,
                 commit_id: str,
                 submit_path: Path,
                 force_rebuild: bool = False,
                 verbose: bool = False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = SubmitState.ADOPTING

        self.master = master
        self.submit_id = submit_id
        self.package_path = package_path
        self.commit_id = commit_id
        self.package = Package(self.package_path, self.commit_id)
        self.build_path = None
        self.submit_path = submit_path
        self.result_path = master.submits_dir / submit_id
        self.force_rebuild = force_rebuild
        self.verbose = verbose
        self.sets = []
        self._submit_update_lock = Lock()
        self._gather_results_lock = Lock()
        self.sets_statuses = {
            SubmitState.ADOPTING: 0,
            SubmitState.SENDING: 0,
            SubmitState.AWAITING_JUDGE: 0,
            SubmitState.SAVING: 0,
            SubmitState.DONE: 0,
            SubmitState.ERROR: 0,
        }
        self._conn = master.connection
        self._conn.exec("INSERT INTO submit_records VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
                        self.submit_id,  # id
                        datetime.now(),  # launch_datetime
                        self.submit_path,  # submit_path
                        self.package_path,  # package_path
                        self.commit_id,  # commit_id
                        self.result_path,  # result_path
                        self.state  # state
                        )
        self.task_submit_dir = self.master.submits_dir / self.submit_id
        if self.task_submit_dir.is_dir():
            shutil.rmtree(self.task_submit_dir)
        self.task_submit_dir.mkdir()
        self.results = {}

    def vprint(self, msg: str):
        if self.verbose:
            print(f'<{self.submit_id}> {msg}')

    def _change_state(self, state: SubmitState, error_msg: str = None):
        self.state = state
        if state == SubmitState.DONE and self.master.delete_records:
                self._conn.exec("DELETE FROM submit_records WHERE id = ?", self.submit_id, )
            # TODO: Check if following exits thread.
            # self.master.close_submit()
        else:
            self._conn.exec("UPDATE submit_records SET state=?, error_msg=? WHERE id=?",
                            state, error_msg, self.submit_id)

        if self.verbose:
            self.vprint(f'Changed state to {state.value} ({state.name})')

    @property
    def kolejka_client(self) -> Path:
        return self.package.build_path(BUILD_NAMESPACE) / 'common' / 'kolejka-client'

    @property
    def kolejka_judge(self) -> Path:
        return self.package.build_path(BUILD_NAMESPACE) / 'common' / 'kolejka-judge'

    @property
    def judge_py(self) -> Path:
        return self.package.build_path(BUILD_NAMESPACE) / 'common' / 'judge.py'

    def set_submit_update(self, status: SubmitState):
        self._submit_update_lock.acquire()
        self.sets_statuses[status] += 1
        self._submit_update_lock.release()
        if self.sets_statuses[status] == len(self.sets):
            self._change_state(status)

    def close_set_submit(self, set_name: str, results: SetResult):
        with self._gather_results_lock:
            self.results[set_name] = results
        if len(self.results) == len(self.sets):
            self._change_state(SubmitState.DONE)
            # TODO: verify
            self._send_to_baca(BACA_URL, BACA_PASSWORD)

    def _call_for_update(self, success: bool, msg: str = None):
        pass

    def _fill_sets(self):
        sets = self.package.sets()
        for t_set in sets:
            self.sets.append(SetSubmit(self.master,
                                       self,
                                       self.submit_id,
                                       self.package,
                                       t_set['name'],
                                       self.submit_path))

    def _build_package(self):
        if self.force_rebuild:
            self.master.refresh_kolejka_src()

        build_pkg = Builder(self.package)
        build_pkg.build()

    def _check_build(self):
        # TODO: Consult package checking
        return True

    def _send_to_baca(self, baca_url: str, password: str) -> None:
        if self.state != SubmitState.DONE:
            raise ValueError(f"Submit state has to be 'DONE' not '{repr(self.state)}'.")
        message = BrokerToBaca(
            pass_hash=make_hash(password, self.submit_id),
            submit_id=self.submit_id,
            results=deepcopy(self.results)  # TODO: verify if deepcopy is appropriate here
        )
        r = requests.post(url=f'{baca_url}/result/{self.submit_id}', json=message.serialize())
        if r.status_code != 200:
            raise ConnectionError(f"Results for TaskSubmit with id {self.submit_id} could not be send.")
        self._change_state(SubmitState.RETURNED)

    def process(self):
        self._change_state(SubmitState.AWAITING_PREPROC)
        self._fill_sets()

        if (not self.package.check_build(BUILD_NAMESPACE)) or self.force_rebuild:
            self._change_state(SubmitState.BUILDING)
            self._build_package()

        self._change_state(SubmitState.CHECKING)
        if not self._check_build():
            self._change_state(SubmitState.CANCELED)
            self._call_for_update(success=False, msg='Package check error')
            return

        self._change_state(SubmitState.SENDING)
        for s in self.sets:
            s.start()

    def run(self):
        try:
            self.process()
        except Exception as e:
            self._change_state(SubmitState.ERROR, str(e))


class SetSubmit(Thread):
    class KOLEJKACommunicationFailed(Exception):
        pass

    def __init__(self,
                 master: BrokerMaster,
                 task_submit: TaskSubmit,
                 submit_id: str,
                 package: Package,
                 set_name: str,
                 submit_path: Path,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = SubmitState.ADOPTING

        self.master = master
        self.task_submit = task_submit
        self.submit_id = submit_id
        self.package = package
        self.set_name = set_name
        self.callback_url = None

        self.submit_path = submit_path
        self._conn = master.connection
        self.submit_http_server = master.kolejka_manager

        self._conn.exec("INSERT INTO set_submit_records VALUES (NULL, ?, ?, NULL, ?)",
                        self.submit_id, self.set_name, self.state)
        self.task_dir = self.task_submit.task_submit_dir / f'{self.set_name}.task'
        self.result_dir = self.task_submit.task_submit_dir / f'{self.set_name}.result'

        if sys.platform.startswith('win'):
            self.python_call = 'py'
        else:
            self.python_call = 'python3'

        # self._call_for_update()

    def vprint(self, msg: str):
        self.task_submit.vprint(f'[{self.set_name}] {msg}')

    def _call_for_update(self):
        self.task_submit.set_submit_update(self.state)

    def _change_state(self, state: SubmitState, error_msg: str = None):
        # TODO: Add state monitoring for parent (task) submit
        self.state = state
        if state == SubmitState.DONE and self.master.delete_records:
            self._conn.exec("DELETE FROM set_submit_records WHERE submit_id=? AND set_name=?",
                            self.submit_id, self.set_name)
            # TODO: Check if following exits thread.
        elif state == SubmitState.DONE or state == SubmitState.ERROR:
            self.task_submit.close_set_submit(self.set_name)
        else:
            self._conn.exec("UPDATE set_submit_records SET state=?, error_msg=? WHERE submit_id=? AND set_name=?",
                            state, error_msg, self.submit_id, self.set_name)
        if self.task_submit.verbose:
            self.vprint(f'Changed state to {state.value} ({state.name})')
        self._call_for_update()

    def _send_submit(self):
        set_id = f'{self.submit_id}_{self.set_name}'
        self.submit_http_server.add_submit(set_id)
        self.callback_url = self.master.broker_server.get_kolejka_callback_url(set_id)
        # TODO: check if the above is OK

        cmd_judge = [self.python_call, self.task_submit.kolejka_judge,
                     'task',
                     '--callback', self.callback_url,
                     '--library-path', self.task_submit.kolejka_judge,
                     self.task_submit.judge_py,
                     self.package.build_path(BUILD_NAMESPACE) / self.set_name / "tests.yaml",
                     self.submit_path,
                     self.task_dir]
        cmd_client = [self.python_call, self.task_submit.kolejka_client,
                      '--config-file', KOLEJKA_CONF,
                      'task', 'put',
                      self.task_dir]

        # kolejka-client result get <result_dir>

        cmd_judge = list(_translate_paths(*cmd_judge))
        cmd_client = list(_translate_paths(*cmd_client))

        judge_status = subprocess.run(cmd_judge)
        if judge_status.returncode != 0:
            raise self.KOLEJKACommunicationFailed('KOLEJKA judge failed to create task.')

        client_status = subprocess.run(cmd_client, capture_output=True)
        self.result_code = client_status.stdout.decode('utf-8').strip()

        if client_status.returncode != 0:
            raise self.KOLEJKACommunicationFailed('KOLEJKA client failed to communicate with KOLEJKA server.')

    def _await_results(self, timeout: float = -1, active_wait: bool = False) -> bool:
        if not active_wait:
            self.submit_http_server.await_submit(self.set_submit_url, timeout)

        result_get = [self.python_call,
                      self.task_submit.kolejka_client,
                      '--config-file', KOLEJKA_CONF,
                      'result', 'get',
                      self.result_code,
                      self.result_dir]

        result_get = list(_translate_paths(*result_get))

        result_status = subprocess.run(result_get, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if not active_wait:
            if result_status.returncode != 0:
                raise self.KOLEJKACommunicationFailed('KOLEJKA client failed to communicate with KOLEJKA server.')
        else:
            time_track = 0
            while result_status.returncode != 0:
                sleep(self.task_submit.TIMEOUT_STEP)
                time_track += self.task_submit.TIMEOUT_STEP
                result_status = subprocess.run(result_get, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if 0 < timeout < time_track:
                    raise self.KOLEJKACommunicationFailed('Reached timeout while waiting for KOLEJKA response.')

        return True

    def _parse_results(self) -> SetResult:
        # TODO: test code
        # TODO: add assertion about status
        results_yaml = self.result_dir / 'results' / 'results.yaml'
        with open(results_yaml) as f:
            content: dict = yaml.load(f, get_loader())
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
        return SetResult(name=self.name, tests=tests)

    def process(self):
        self._change_state(SubmitState.SENDING)
        self._send_submit()

        self._change_state(SubmitState.AWAITING_JUDGE)
        # TODO: consider adding timeout for safety (otherwise if something goes wrong the method may never return)
        self._await_results(timeout=60, active_wait=True)

        self._change_state(SubmitState.SAVING)
        results = self._parse_results()

        self._change_state(SubmitState.DONE)
        self.task_submit.close_set_submit(self.set_name, results)


    def run(self):
        try:
            self.process()
        except Exception as e:
            self._change_state(SubmitState.ERROR, str(e))
            # TODO: error handling for BaCa2 srv
