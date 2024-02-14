import asyncio

from baca2PackageManager.broker_communication import BacaToBroker
from aiologger import Logger

from .messenger import KolejkaMessengerInterface, BacaMessengerInterface, PackageManagerInterface
from .datamaster import DataMasterInterface, SetSubmitInterface, TaskSubmitInterface


class BrokerMaster:

    def __init__(self,
                 data_master: DataMasterInterface,
                 kolejka_messenger: KolejkaMessengerInterface,
                 baca_messenger: BacaMessengerInterface,
                 package_manager: PackageManagerInterface,
                 logger: Logger):
        self.kolejka_messenger = kolejka_messenger
        self.baca_messenger = baca_messenger
        self.data_master = data_master
        self.package_manager = package_manager
        self.logger = logger

    async def trash_task_submit(self, task_submit: TaskSubmitInterface, error: Exception):
        async with task_submit.lock:
            task_submit.change_state(task_submit.TaskState.ERROR, requires=None)
            await self.baca_messenger.send_error(task_submit, str(error))
            self.data_master.delete_task_submit(task_submit)

    async def handle_baca(self, data: BacaToBroker):
        task_submit = self.data_master.new_task_submit(data.submit_id,
                                                       data.package_path,
                                                       data.commit_id,
                                                       data.submit_path)
        try:
            await task_submit.initialise()

            if not await self.package_manager.check_build(task_submit.package) or self.package_manager.force_rebuild:
                await self.package_manager.build_package(task_submit.package)

            await self.process_task_submit(task_submit)
        except Exception as e:
            # log
            await self.trash_task_submit(task_submit, e)
            raise

    async def process_task_submit(self, task_submit: TaskSubmitInterface):

        async def kolejka_send_task(set_submit: SetSubmitInterface):
            async with set_submit.lock:
                set_submit.change_state(set_submit.SetState.SENDING_TO_KOLEJKA,
                                        requires=set_submit.SetState.INITIAL)
                status_code = await self.kolejka_messenger.send(set_submit)
                set_submit.set_status_code(status_code)
                set_submit.change_state(set_submit.SetState.AWAITING_KOLEJKA,
                                        requires=set_submit.SetState.SENDING_TO_KOLEJKA)

        task_submit.change_state(task_submit.TaskState.AWAITING_SETS, requires=task_submit.TaskState.INITIAL)
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(kolejka_send_task(s)) for s in task_submit.set_submits]
        if not all(t.done() for t in tasks):
            raise RuntimeError("Not all tasks finished (this should never happen)")

    async def handle_kolejka(self, submit_id: str):
        try:
            set_submit = self.data_master.get_set_submit(submit_id)
        except self.data_master.DataMasterError as e:
            # log
            raise
        try:
            await self.process_finished_set_submit(set_submit)
        except Exception as e:
            # log
            await self.trash_task_submit(set_submit.task_submit, e)
            raise

    async def process_finished_set_submit(self, set_submit: SetSubmitInterface):

        async with set_submit.lock:
            set_submit.change_state(set_submit.SetState.DONE,
                                    requires=set_submit.SetState.AWAITING_KOLEJKA)
            results = await self.kolejka_messenger.get_results(set_submit, set_submit.get_status_code())
            set_submit.set_result(results)

        task_submit = set_submit.task_submit
        async with task_submit.lock:
            if task_submit.all_checked():
                task_submit.change_state(task_submit.TaskState.DONE,
                                         requires=task_submit.TaskState.AWAITING_SETS)
                await self.baca_messenger.send(task_submit)
                self.data_master.delete_task_submit(task_submit)
