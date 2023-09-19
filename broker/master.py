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
        self.connection = Connection(db_string)
        self.delete_records = delete_records
        self.submits_dir = submits_dir
        self.threads = threads
        self.submits = {}
        self.kolejka_manager = KolejkaCommunicationManager()
        self.broker_server = BrokerIOServer(self.kolejka_manager, self)
        self.timeout_manager = TimeoutManager()

    def __del__(self):
        self.broker_server.close_server()
        self.timeout_manager.stop()


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
        submit = TaskSubmit(self,
                            submit_id,
                            package_path,
                            commit_id,
                            submit_path,
                            force_rebuild=APP_SETTINGS['force_rebuild'],
                            verbose=APP_SETTINGS['verbose'])
        self.submits[submit_id] = submit
        submit.start()

    def submit_results(self, submit_id: str, results):
        pass

    def close_submit(self, submit_id: str):
        if self.submits.get(submit_id) is not None:
            del self.submits[submit_id]
