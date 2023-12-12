import os
import shutil
import stat

import requests as req
from pathlib import Path

from db.connector import Connection
from .submit import TaskSubmit
from .server import KolejkaCommunicationManager, BrokerIOServer

from settings import KOLEJKA_SRC_DIR, APP_SETTINGS
from .timeout import TimeoutManager


class BrokerMaster:
    def __init__(self,
                 db_string: str,
                 submits_dir: Path,
                 delete_records: bool = APP_SETTINGS['delete_records'],
                 threads: int = 2
                 ):
        self.db_string = db_string
        self._is_active = False
        self.connection = None  # To be initialized in start()
        self.delete_records = delete_records
        self.submits_dir = submits_dir
        self.threads = threads
        self.submits = {}
        self.kolejka_manager = KolejkaCommunicationManager()
        self.broker_server = None  # To be initialized in start()
        self.timeout_manager = None  # To be initialized in start()
        self.verbose = APP_SETTINGS['verbose']
        if self.verbose:
            print('Broker master initialized')
            print('Initial settings:')
            for key, value in APP_SETTINGS.items():
                print(f'\t{key}: {value}')

    def __del__(self):
        if self._is_active:
            self.stop()

    @property
    def is_active(self):
        return self._is_active

    def start(self):
        if self._is_active:
            raise RuntimeError('Broker master is already running')
        self.connection = Connection(self.db_string)
        self.broker_server = BrokerIOServer(self.kolejka_manager, self)
        self.timeout_manager = TimeoutManager()
        self._is_active = True

    def stop(self):
        if not self._is_active:
            raise RuntimeError('Broker master is not running')
        self.broker_server.close_server()
        self.timeout_manager.stop()
        self._is_active = False

    def serve_until_interrupted(self):
        if not self._is_active:
            self.start()

        while True:
            inp = input("Enter 'STOP' to stop the broker.\n")
            if inp == 'STOP':
                break

        self.stop()

    @staticmethod
    def refresh_kolejka_src(add_executable_attr: bool = True):
        if KOLEJKA_SRC_DIR.is_dir():
            shutil.rmtree(KOLEJKA_SRC_DIR)
        KOLEJKA_SRC_DIR.mkdir()

        kolejka_judge = req.get('https://kolejka.matinf.uj.edu.pl/kolejka-judge').content
        kolejka_client = req.get('https://kolejka.matinf.uj.edu.pl/kolejka-client').content

        kolejka_judge_path = KOLEJKA_SRC_DIR / 'kolejka-judge'
        kolejka_client_path = KOLEJKA_SRC_DIR / 'kolejka-client'

        with open(kolejka_judge_path, mode='wb') as judge:
            judge.write(kolejka_judge)
        with open(kolejka_client_path, mode='wb') as client:
            client.write(kolejka_client)

        if add_executable_attr:
            current_judge = os.stat(kolejka_judge_path)
            current_client = os.stat(kolejka_client_path)

            os.chmod(kolejka_judge_path, current_judge.st_mode | stat.S_IEXEC)
            os.chmod(kolejka_client_path, current_client.st_mode | stat.S_IEXEC)

    def new_submit(self,
                   submit_id: str,
                   package_path: Path,
                   commit_id: str,
                   submit_path: Path):
        try:
            submit = TaskSubmit(self,
                                submit_id,
                                package_path,
                                commit_id,
                                submit_path,
                                force_rebuild=APP_SETTINGS['force_rebuild'],
                                verbose=APP_SETTINGS['verbose'])
        except TaskSubmit.JudgingError:
            return
        self.submits[submit_id] = submit
        submit.start()

    def submit_results(self, submit_id: str, results):
        pass

    def close_submit(self, submit_id: str):
        if self.submits.get(submit_id) is not None:
            del self.submits[submit_id]
