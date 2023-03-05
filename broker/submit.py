from threading import Thread
from pathlib import Path
from enum import Enum
from datetime import datetime
from yaml import safe_load
from db.connector import Connection


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


class Submit(Thread):
    def __init__(self,
                 master,
                 submit_id: str,
                 package_path: Path,
                 submit_path: Path):
        super().__init__()
        self.state = SubmitState.ADOPTING

        self.master = master
        self.submit_id = submit_id
        self.package_path = package_path
        self.submit_path = submit_path
        self.result_path = master.results_dir / submit_id
        self._conn = master.connection

        self._conn.exec("INSERT INTO submit_records VALUES (?, ?, ?, ?, ?, NULL, ?)",
                        (submit_id, datetime.now(), submit_path, package_path, self.result_path, SubmitState.ADOPTING))

    def _change_state(self, state: SubmitState, error_msg: str = None):
        self.state = state
        if state == SubmitState.DONE and self.master.delete_records:
            self._conn.exec("DELETE FROM submit_records WHERE id = ?", (self.submit_id,))
        else:
            self._conn.exec("UPDATE submit_records SET state=?, error_msg=? WHERE id=?",
                            (state, error_msg, self.submit_id))

    @property
    def _is_built(self):
        build_dir = self.package_path / ".build"
        return build_dir.exists() and build_dir.is_dir()

    def _build_package(self):
        pass

    def _check_build(self):
        # TODO: Consult package checking
        return True

    def _send_submit(self):
        pass

    def _await_results(self):
        pass

    def _call_for_update(self, success: bool, msg: str = None):
        pass

    def process(self):
        if not self._is_built:
            self._change_state(SubmitState.BUILDING)
            self._build_package()

        self._change_state(SubmitState.CHECKING)
        if not self._check_build():
            self._change_state(SubmitState.CANCELED)
            self._call_for_update(success=False, msg='Package check error')
            return

        self._change_state(SubmitState.SENDING)
        self._send_submit()

        self._change_state(SubmitState.AWAITING_JUDGE)
        self._await_results()

        self._change_state(SubmitState.SAVING)
        self._call_for_update(success=True)

        self._change_state(SubmitState.DONE)

    def run(self):
        try:
            self.process()
        except Exception as e:
            self._change_state(SubmitState.ERROR, str(e))
            # TODO: error handling for BaCa2 srv
