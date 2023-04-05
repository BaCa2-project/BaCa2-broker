from pathlib import Path

from db.connector import Connection
from .submit import Submit


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
        self.submits = {}

    def new_submit(self,
                   submit_id: str,
                   package_path: Path,
                   submit_path: Path):
        submit = Submit(self, submit_id, package_path, submit_path)
        self.submits[submit_id] = submit
        submit.start()

    def close_submit(self, submit_id: str):
        if self.submits.get(submit_id) is not None:
            del self.submits[submit_id]

