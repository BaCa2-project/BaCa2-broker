from pathlib import Path
from queue import PriorityQueue
from threading import Semaphore

from db.connector import Connection
from .submit import Submit

class Scheduler:
    def __init__(self,
                 threads: int):
        self.threads = threads
        self.locked_dirs = set()
        self.wait_list = PriorityQueue()

    def is_submit_locked(self,
                         submit: Submit):
        # if submit.package_path is
        pass


    def schedule_submit(self, submit: Submit):
        pass

class BrokerMaster:
    def __init__(self,
                 db_string: str,
                 results_dir: Path,
                 delete_records: bool = True,
                 threads: int = 2
                 ):
        self.connection = Connection(db_string)
        self.delete_records = delete_records
        self.results_dir = results_dir
        self.threads = threads


    def new_submit(self,
                   submit_id: str,
                   package_path: Path,
                   submit_path: Path):
        pass
