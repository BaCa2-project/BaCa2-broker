from abc import ABC, abstractmethod
import logging
from pathlib import Path

from baca2PackageManager.broker_communication import BacaToBroker

from .broker.messenger import KolejkaMessengerActiveWait
from .broker.master import BrokerMaster


class Handler(ABC):

    @abstractmethod
    async def handle_baca(self, data: BacaToBroker):
        pass


class PassiveHandler(Handler):

    def __init__(self, broker_master: BrokerMaster, log: logging.Logger):
        self.master = broker_master
        self.data_master = self.master.data_master
        self.logger = log

    async def handle_baca(self, data: BacaToBroker):
        try:
            task_submit = self.data_master.new_task_submit(data.submit_id,
                                                           Path(data.package_path),
                                                           data.commit_id,
                                                           Path(data.submit_path))
        except self.data_master.DataMasterError as e:
            self.logger.error("Task submit '%s' not created: (%s)", data.submit_id, str(e))
            return

        try:
            await task_submit.initialise()
            await self.master.process_package(task_submit.package)
            await self.master.process_new_task_submit(task_submit)
        except Exception as e:
            self.logger.error("Error while processing task submit '%s': %s", data.submit_id, str(e))
            await self.master.trash_task_submit(task_submit, e)
            return
        else:
            self.logger.log(logging.INFO, "Task submit '%s' started successfully", data.submit_id)

    async def handle_kolejka(self, submit_id: str):
        try:
            set_submit = self.data_master.get_set_submit(submit_id)
        except self.data_master.DataMasterError as e:
            self.logger.error("Set submit '%s' not found: %s", submit_id, str(e))
            return
        try:
            await self.master.process_finished_set_submit(set_submit)
        except Exception as e:
            self.logger.error("Error while processing set submit '%s': %s", submit_id, str(e))
            await self.master.trash_task_submit(set_submit.task_submit, e)
            return
        try:
            task_submit = set_submit.task_submit
            async with task_submit.lock:
                if task_submit.all_checked() and task_submit.state == task_submit.TaskState.AWAITING_SETS:
                    self.logger.log(logging.INFO,
                                    "All sets checked for task submit '%s', now sending to BaCa2",
                                    task_submit.submit_id)
                    await self.master.process_finished_task_submit(task_submit)
        except Exception as e:
            self.logger.error("Error while finishing task submit '%s': %s", submit_id, str(e))
            await self.master.trash_task_submit(set_submit.task_submit, e)
            return


class ActiveHandler(Handler):

    def __init__(self, broker_master: BrokerMaster, kolejka_messenger: KolejkaMessengerActiveWait, log: logging.Logger):
        self.master = broker_master
        self.kolejka_messenger = kolejka_messenger
        # assert isinstance(self.kolejka_messenger, KolejkaMessengerActiveWait)
        assert self.master.kolejka_messenger is self.kolejka_messenger
        self.data_master = self.master.data_master
        self.logger = log

    async def handle_baca(self, data: BacaToBroker):
        try:
            task_submit = self.data_master.new_task_submit(data.submit_id,
                                                           Path(data.package_path).resolve(),
                                                           data.commit_id,
                                                           Path(data.submit_path).resolve())
        except self.data_master.DataMasterError as e:
            self.logger.error("Task submit '%s' not created: (%s)", data.submit_id, str(e))
            return

        try:
            await task_submit.initialise()
            await self.master.process_package(task_submit.package)
            await self.master.process_new_task_submit(task_submit)
            for s in task_submit.set_submits:
                await self.master.process_finished_set_submit(s)
            self.logger.log(logging.INFO,
                            "All sets checked for task submit '%s', now sending to BaCa2",
                            data.submit_id)
            await self.master.process_finished_task_submit(task_submit)
        except Exception as e:
            self.logger.error("Error while processing task submit '%s': %s", data.submit_id, str(e), exc_info=True)
            await self.master.trash_task_submit(task_submit, e)
            return
        else:
            self.logger.log(logging.INFO, "Task submit '%s' processed successfully", data.submit_id)
