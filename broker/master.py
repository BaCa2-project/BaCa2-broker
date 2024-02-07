from baca2PackageManager.broker_communication import BacaToBroker

from .messenger import KolejkaMessengerInterface, BacaMessengerInterface
from .datamaster import DataMasterInterface, TaskSubmit


class BrokerMaster:

    def __init__(self,
                 data_master: DataMasterInterface,
                 kolejka_messenger: KolejkaMessengerInterface,
                 baca_messenger: BacaMessengerInterface):
        self.kolejka_messenger = kolejka_messenger
        self.baca_messenger = baca_messenger
        self.data_master = data_master

    async def handle_baca(self, data: BacaToBroker):
        task_submit = self.data_master.new_task_submit(data.submit_id,
                                                       data.package_path,
                                                       data.commit_id,
                                                       data.submit_path)
        try:
            await self._handle_baca_inner(task_submit)
        except Exception as e:
            await self.baca_messenger.send_error(task_submit, e)
            task_submit.change_state(task_submit.TaskState.ERROR)
            self.data_master.delete_task_submit(task_submit)
            raise

    async def _handle_baca_inner(self, task_submit: TaskSubmit):
        await task_submit.initialise()

        # if (not self.package.check_build(BUILD_NAMESPACE)) or self.force_rebuild:  # TODO
        #     self._build_package()

        for s in task_submit.set_submits:
            status_code = await self.kolejka_messenger.send(s)
            s.set_status_code(status_code)
            s.change_state(s.SetState.AWAITING_KOLEJKA)

        task_submit.change_state(task_submit.TaskState.AWAITING_SETS)

    async def handle_kolejka(self, submit_id: str):
        try:
            set_submit = self.data_master.get_set_submit(submit_id)
        except self.data_master.DataMasterError:
            # TODO: log
            raise
        try:
            await self._handle_kolejka_inner(submit_id)
        except Exception as e:
            self.data_master.delete_task_submit(set_submit.task_submit)
            await self.baca_messenger.send_error(set_submit.task_submit, e)
            raise

    async def _handle_kolejka_inner(self, submit_id: str):
        set_submit = self.data_master.get_active_set_submit(submit_id)
        results = await self.kolejka_messenger.get_results(set_submit, set_submit.get_status_code())
        set_submit.set_result(results)
        set_submit.change_state(set_submit.SetState.DONE)

        if set_submit.task_submit.all_checked():
            await self.baca_messenger.send(set_submit.task_submit)
            set_submit.task_submit.change_state(set_submit.task_submit.TaskState.DONE)
