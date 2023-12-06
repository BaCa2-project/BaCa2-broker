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
from settings import (BUILD_NAMESPACE, KOLEJKA_CONF, BACA_PASSWORD, BACA_RESULTS_URL, BACA_ERROR_URL,
                      APP_SETTINGS, BACA_SEND_TRIES, BACA_SEND_INTERVAL, DELETE_ERROR_SUBMITS)

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


class CallbackStatus(Enum):
    NOT_SENT = 0
    SENT = 1
    RECEIVED = 2
    TIMEOUT = 3
    ERROR = 4


class TaskSubmit(Thread):
    class BaCa2CommunicationError(Exception):
        pass

    class JudgingError(Exception):
        pass

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
            SubmitState.CANCELED: 0,
        }
        self._conn = master.connection
        if self._submit_in_db():
            raise self.JudgingError(f'Submit with id {self.submit_id} already exists in db.')
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
        self.active_wait = APP_SETTINGS['active_wait']

    @property
    def status(self) -> SubmitState:
        return self.state

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
        end_statuses = (self.sets_statuses[SubmitState.ERROR] +
                        self.sets_statuses[SubmitState.DONE] +
                        self.sets_statuses[SubmitState.CANCELED])
        if self.sets_statuses[status] == len(self.sets) and status not in (SubmitState.DONE, SubmitState.SAVING):
            self._change_state(status)
        elif end_statuses == len(self.sets):
            ok_statuses = self.sets_statuses[SubmitState.DONE]
            if ok_statuses != len(self.sets):
                raise self.JudgingError(f'Some sets ended with an error ({ok_statuses}/{len(self.sets)} OK)')

    def close_set_submit(self, set_name: str, results: SetResult):
        with self._gather_results_lock:
            self.results[set_name] = results

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

    def _check_results(self):
        if self.state != SubmitState.SAVING:
            raise self.JudgingError(f"Submit state has to be 'SAVING' not '{self.state.name}'.")
        for s in self.sets:
            if s.state != SubmitState.DONE:
                raise self.JudgingError(f"SetSubmit state has to be 'DONE' not '{s.state.name}'.")
        return True

    def _send_to_baca(self, baca_url: str, password: str) -> bool:
        message = BrokerToBaca(
            pass_hash=make_hash(password, self.submit_id),
            submit_id=self.submit_id,
            results=deepcopy(self.results)
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

    def _send_error_to_baca(self, baca_url: str, password: str, error_msg: str) -> bool:
        message = BrokerToBacaError(
            pass_hash=make_hash(password, self.submit_id),
            submit_id=self.submit_id,
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

        for s in self.sets:
            s.join()

        self._change_state(SubmitState.SAVING)
        self._check_results()
        for i in range(BACA_SEND_TRIES):
            if self._send_to_baca(BACA_RESULTS_URL, BACA_PASSWORD):
                break
            sleep(BACA_SEND_INTERVAL)
        else:
            raise self.BaCa2CommunicationError(f"Results for TaskSubmit with id {self.submit_id} could not be send.")

        self._change_state(SubmitState.DONE)

    def _submit_in_db(self) -> bool:
        tmp = self._conn.select(
            f"SELECT * FROM submit_records WHERE id=?", 'one', self.submit_id)
        if (
                DELETE_ERROR_SUBMITS
                and tmp is not None
                and self.master.submits[self.submit_id].status == SubmitState.ERROR
        ):
            self._conn.exec("DELETE FROM submit_records WHERE id=?", self.submit_id)
            return False
        else:
            return tmp is not None

    def run(self):
        try:
            self.process()
        except self.BaCa2CommunicationError as e:
            self._change_state(SubmitState.ERROR, f'{e.__class__.__name__}: {e}')
        except Exception as e:
            self._send_error_to_baca(BACA_ERROR_URL, BACA_PASSWORD, str(e))
            self._change_state(SubmitState.ERROR, f'{e.__class__.__name__}: {e} \n\n '
                                                  f'{e.__traceback__.tb_lineno} {e.__traceback__.tb_lasti}')


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
        self.set_submit_id = f'{self.submit_id}_{self.set_name}'

        self.submit_path = submit_path
        self._conn = master.connection
        self.kolejka_manager = master.kolejka_manager

        self._conn.exec("INSERT INTO set_submit_records VALUES (NULL, ?, ?, NULL, ?)",
                        self.submit_id, self.set_name, self.state)
        self.task_dir = self.task_submit.task_submit_dir / f'{self.set_name}.task'
        self.result_dir = self.task_submit.task_submit_dir / f'{self.set_name}.result'
        self.results = None

        if sys.platform.startswith('win'):
            self.python_call = 'py'
        else:
            self.python_call = 'python3'

        self.callback_status = CallbackStatus.NOT_SENT
        # self._call_for_update()

    def vprint(self, msg: str):
        self.task_submit.vprint(f'[{self.set_name}] {msg}')

    def _call_for_update(self):
        self.task_submit.set_submit_update(self.state)

    def _change_state(self, state: SubmitState, error_msg: str = None):
        self.state = state
        if state == SubmitState.DONE and self.master.delete_records:
            self._conn.exec("DELETE FROM set_submit_records WHERE submit_id=? AND set_name=?",
                            self.submit_id, self.set_name)
            # TODO: Check if following exits thread.
        else:
            self._conn.exec("UPDATE set_submit_records SET state=?, error_msg=? WHERE submit_id=? AND set_name=?",
                            state, error_msg, self.submit_id, self.set_name)
        if self.task_submit.verbose:
            self.vprint(f'Changed state to {state.value} ({state.name})')
        self._call_for_update()

    def _send_submit(self):
        set_id = f'{self.submit_id}_{self.set_name}'
        if not self.task_submit.active_wait:
            self.kolejka_manager.add_submit(set_id)
            self.callback_url = self.master.broker_server.get_kolejka_callback_url(set_id)
        else:
            self.callback_url = 'localhost'

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
        self.callback_status = CallbackStatus.SENT

    def results_get(self) -> bool:
        result_get = [self.python_call,
                      self.task_submit.kolejka_client,
                      '--config-file', KOLEJKA_CONF,
                      'result', 'get',
                      self.result_code,
                      self.result_dir]

        result_get = list(_translate_paths(*result_get))

        result_status = subprocess.run(result_get, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if result_status.returncode == 0:
            return True
        else:
            return False

    def success_ping(self, success: bool) -> None:
        if success:
            self.callback_status = CallbackStatus.RECEIVED
        else:
            self.callback_status = CallbackStatus.TIMEOUT

    def _await_results(self) -> bool:
        from settings import APP_SETTINGS

        await_results_lock = Lock()
        if self.task_submit.active_wait:
            self.master.timeout_manager.add_active_wait(self.set_submit_id,
                                                        await_results_lock,
                                                        wait_action=self.results_get,
                                                        success_ping=self.success_ping, )
            await_results_lock.acquire()
            self.master.timeout_manager.remove_timeout(self.set_submit_id)
        else:
            self.success_ping(self.master.kolejka_manager.await_submit(
                self.set_submit_id,
                timeout=APP_SETTINGS['default_timeout'].total_seconds())
            )

            results_received = False
            if self.callback_status == CallbackStatus.TIMEOUT:
                results_received = self.results_get()
            if results_received:
                self.success_ping(True)

        if self.callback_status == CallbackStatus.TIMEOUT:
            raise self.KOLEJKACommunicationFailed('KOLEJKA callback timeout')
        elif self.callback_status == CallbackStatus.ERROR:
            raise self.KOLEJKACommunicationFailed('KOLEJKA callback error')
        elif self.callback_status != CallbackStatus.RECEIVED:
            raise self.KOLEJKACommunicationFailed('KOLEJKA callback unknown error')

        return True

    def _parse_results(self) -> SetResult:
        # TODO: add assertion about status
        results_yaml = self.result_dir / 'results' / 'results.yaml'
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
        return SetResult(name=self.set_name, tests=tests)

    def process(self):
        self._change_state(SubmitState.SENDING)
        self._send_submit()

        self._change_state(SubmitState.AWAITING_JUDGE)
        self._await_results()

        self._change_state(SubmitState.SAVING)
        self.results = self._parse_results()

        self.task_submit.close_set_submit(self.set_name, self.results)

        self._change_state(SubmitState.DONE)

    def run(self):
        try:
            self.process()
        except Exception as e:
            self._change_state(SubmitState.ERROR, f'{e.__class__.__name__}: {e}')
            # TODO: error handling for BaCa2 srv
