"""Main class for handling broker's logic."""
import asyncio
import logging
from datetime import timedelta

from baca2PackageManager import Package

from .messenger import KolejkaMessengerInterface, BacaMessengerInterface, PackageManagerInterface
from .datamaster import DataMasterInterface, SetSubmitInterface, TaskSubmitInterface


class BrokerMaster:
    """Main class for handling broker's logic."""

    def __init__(self,
                 data_master: DataMasterInterface,
                 kolejka_messenger: KolejkaMessengerInterface,
                 baca_messenger: BacaMessengerInterface,
                 package_manager: PackageManagerInterface,
                 logger: logging.Logger):
        self.kolejka_messenger = kolejka_messenger
        self.baca_messenger = baca_messenger
        self.data_master = data_master
        self.package_manager = package_manager
        self.logger = logger

    async def process_new_task_submit(self, task_submit: TaskSubmitInterface):
        """Sends all sets to kolejka and changes state of task submit to AWAITING_SETS."""

        async def kolejka_send_task(set_submit: SetSubmitInterface):
            async with set_submit.lock:
                set_submit.change_state(set_submit.SetState.SENDING_TO_KOLEJKA,
                                        requires=set_submit.SetState.INITIAL)
                await self.kolejka_messenger.send(set_submit)
                set_submit.change_state(set_submit.SetState.AWAITING_KOLEJKA,
                                        requires=set_submit.SetState.SENDING_TO_KOLEJKA)

        async with task_submit.lock:
            task_submit.change_state(task_submit.TaskState.AWAITING_SETS, requires=task_submit.TaskState.INITIAL)
            tasks = [kolejka_send_task(s) for s in task_submit.set_submits]
            await asyncio.gather(*tasks)

    async def trash_task_submit(self, task_submit: TaskSubmitInterface, error: Exception | None):
        """
        Changes state of task submit to ERROR, sends error message to BaCa2 if error != None
         and deletes task submit from database.
        """
        self.logger.info("Trashing task submit '%s'", task_submit.submit_id)
        async with task_submit.lock:
            task_submit.change_state(task_submit.TaskState.ERROR, requires=None)
            task_submit.change_set_states(SetSubmitInterface.SetState.ERROR, requires=None)
            self.data_master.delete_task_submit(task_submit)
            if error is not None:
                await self.baca_messenger.send_error(task_submit, str(error))

    async def process_finished_set_submit(self, set_submit: SetSubmitInterface):
        """Gets results from kolejka and changes state of set submit to DONE."""
        async with set_submit.lock:
            set_submit.change_state(set_submit.SetState.WAITING_FOR_RESULTS,
                                    requires=set_submit.SetState.AWAITING_KOLEJKA)
            await self.kolejka_messenger.get_results(set_submit)
            set_submit.change_state(set_submit.SetState.DONE,
                                    requires=set_submit.SetState.WAITING_FOR_RESULTS)
            self.logger.info("Set submit '%s' finished in %s",
                             set_submit.submit_id, set_submit.mod_date - set_submit.creation_date)

    async def process_finished_task_submit(self, task_submit: TaskSubmitInterface):
        """Sends task submit to BaCa2 and deletes it from database. All set submits must be checked before calling."""
        if not task_submit.all_checked():
            raise ValueError("Not all sets checked")
        task_submit.change_state(task_submit.TaskState.DONE,
                                 requires=task_submit.TaskState.AWAITING_SETS)
        await self.baca_messenger.send(task_submit)
        self.logger.info("Task submit '%s' finished in %s",
                         task_submit.submit_id, task_submit.mod_date - task_submit.creation_date)
        self.data_master.delete_task_submit(task_submit)

    async def if_all_checked_process_finished_task_submit(self, task_submit: TaskSubmitInterface):
        async with task_submit.lock:
            if task_submit.all_checked() and task_submit.state == task_submit.TaskState.AWAITING_SETS:
                self.logger.info("All sets checked for task submit '%s', now sending to BaCa2",
                                 task_submit.submit_id)
                await self.process_finished_task_submit(task_submit)

    async def process_package(self, package: Package):
        """Builds package if needed."""
        if not await self.package_manager.check_build(package) or self.package_manager.force_rebuild:
            self.logger.info("Building package '%s'", package.name)
            await self.package_manager.build_package(package)
            self.logger.info("Package '%s' built successfully", package.name)

    async def _deletion_daemon_body(self, task_submit_timeout: timedelta):
        self.logger.info("Running deletion daemon")
        to_be_deleted = []
        for task_submit in self.data_master.task_submits.values():
            if task_submit.mod_date - task_submit.creation_date >= task_submit_timeout:
                to_be_deleted.append(task_submit)

        if to_be_deleted:
            self.logger.info("Found %s old submits that will now be deleted: %s",
                             len(to_be_deleted), [sub.submit_id for sub in to_be_deleted])
        else:
            self.logger.info("No old submits to delete")

        for task_submit in to_be_deleted:
            await self.trash_task_submit(task_submit, None)

    async def deletion_daemon(self, task_submit_timeout: timedelta, interval: int):
        while True:
            await self._deletion_daemon_body(task_submit_timeout)
            await asyncio.sleep(interval)

    async def start_daemons(self, task_submit_timeout: timedelta, interval: int):
        await asyncio.gather(self.deletion_daemon(task_submit_timeout, interval))
