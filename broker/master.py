import asyncio

from baca2PackageManager.broker_communication import BacaToBroker
from aiologger import Logger

from .messenger import KolejkaMessengerInterface, BacaMessengerInterface
from .datamaster import DataMasterInterface, SetSubmit


class BrokerMaster:

    def __init__(self,
                 data_master: DataMasterInterface,
                 kolejka_messenger: KolejkaMessengerInterface,
                 baca_messenger: BacaMessengerInterface,
                 logger: Logger):
        self.kolejka_messenger = kolejka_messenger
        self.baca_messenger = baca_messenger
        self.data_master = data_master
        self.logger = logger

    async def _kolejka_send_task(self, set_submit: SetSubmit):
        set_submit.change_state(set_submit.SetState.SENDING_TO_KOLEJKA)
        status_code = await self.kolejka_messenger.send(set_submit)
        set_submit.set_status_code(status_code)
        set_submit.change_state(set_submit.SetState.AWAITING_KOLEJKA)

    async def handle_baca(self, data: BacaToBroker):
        task_submit = self.data_master.new_task_submit(data.submit_id,
                                                       data.package_path,
                                                       data.commit_id,
                                                       data.submit_path)
        try:
            await task_submit.initialise()
            task_submit.change_state(task_submit.TaskState.AWAITING_SETS)
            # if (not self.package.check_build(BUILD_NAMESPACE)) or self.force_rebuild:  # TODO
            #     self._build_package()
            async with asyncio.TaskGroup() as tg:
                tasks = []
                for s in task_submit.set_submits:
                    task = tg.create_task(self._kolejka_send_task(s))
                    tasks.append(task)
            if not all(t.done() for t in tasks):
                raise Exception("Not all tasks finished")  # TODO

        except Exception as e:
            await self.baca_messenger.send_error(task_submit, e)
            task_submit.change_state(task_submit.TaskState.ERROR)
            self.data_master.delete_task_submit(task_submit)
            raise

    async def handle_kolejka(self, submit_id: str):
        try:
            set_submit = self.data_master.get_set_submit(submit_id)
        except self.data_master.DataMasterError as e:
            await self.logger.error(str(e))
            raise

        if set_submit.state == set_submit.SetState.AWAITING_KOLEJKA:
            await self.logger.error(f"Set submit {submit_id} in invalid state")
            raise Exception(f"Set submit {submit_id} in invalid state")  # TODO

        set_submit.change_state(set_submit.SetState.DONE)
        try:
            results = await self.kolejka_messenger.get_results(set_submit, set_submit.get_status_code())
            set_submit.set_result(results)

            if set_submit.task_submit.all_checked():
                task_submit = set_submit.task_submit
                await self.baca_messenger.send(task_submit)
                task_submit.change_state(task_submit.TaskState.DONE)
                self.data_master.delete_task_submit(task_submit)

        except Exception as e:
            task_submit = set_submit.task_submit
            await self.baca_messenger.send_error(task_submit, e)
            task_submit.change_state(task_submit.TaskState.ERROR)
            self.data_master.delete_task_submit(task_submit)
            raise
