import shutil

import asyncio
import logging
import unittest
from pathlib import Path
from time import sleep

from settings import SUBMITS_DIR, BUILD_NAMESPACE, KOLEJKA_CONF, KOLEJKA_SRC_DIR

from app.broker.messenger import KolejkaMessenger, PackageManager, KolejkaMessengerActiveWait
from app.broker.datamaster import TaskSubmit, DataMaster, SetSubmit


class KolejkaMessengerTest(unittest.TestCase):
    test_dir = Path(__file__).parent.parent

    def setUp(self):
        self.logger = logging.Logger('test')
        self.logger.addHandler(logging.StreamHandler())
        self.data_master = DataMaster(TaskSubmit, SetSubmit, self.logger)
        self.package_path = self.test_dir / 'resources' / '1'
        self.submit_path = self.test_dir / 'resources' / '1' / '1' / 'prog' / 'solution.cpp'
        self.kolejka_messanger = KolejkaMessenger(
            submits_dir=SUBMITS_DIR,
            build_namespace=BUILD_NAMESPACE,
            kolejka_conf=KOLEJKA_CONF,
            kolejka_callback_url_prefix='http://127.0.0.1/',
            logger=self.logger
        )
        self.package_manager = PackageManager(
            kolejka_src_dir=KOLEJKA_SRC_DIR,
            build_namespace=BUILD_NAMESPACE,
            force_rebuild=False,
        )
        self.package_manager.refresh_kolejka_src()

        shutil.rmtree(SUBMITS_DIR / 'submit_id', ignore_errors=True)
        shutil.rmtree(SUBMITS_DIR / '1', ignore_errors=True)

    def test_send_kolejka(self):
        task_submit = self.data_master.new_task_submit(task_submit_id="1",
                                                       package_path=self.package_path,
                                                       commit_id="1",
                                                       submit_path=self.submit_path)
        asyncio.run(task_submit.initialise())
        asyncio.run(self.package_manager.build_package(task_submit.package))
        set_submit = task_submit.set_submits[0]
        asyncio.run(self.kolejka_messanger.send(set_submit))
        print(set_submit.get_status_code())
        self.assertIsNotNone(set_submit.get_status_code())
        self.assertNotEqual('', set_submit.get_status_code())

    def test_kolejka_receive(self):
        task_submit = self.data_master.new_task_submit(task_submit_id="submit_id",
                                                       package_path=self.package_path,
                                                       commit_id="1",
                                                       submit_path=self.submit_path)
        asyncio.run(task_submit.initialise())
        asyncio.run(self.package_manager.build_package(task_submit.package))
        set_submit = task_submit.set_submits[0]
        asyncio.run(self.kolejka_messanger.send(set_submit))
        sleep(10)
        asyncio.run(self.kolejka_messanger.get_results(set_submit))
        self.assertEqual(set_submit.get_result().tests['1'].status, 'MEM')
        print(f'{set_submit.get_result()=}')

    def test_kolejka_active_wait(self):
        kolejka_messanger = KolejkaMessengerActiveWait(
            submits_dir=SUBMITS_DIR,
            build_namespace=BUILD_NAMESPACE,
            kolejka_conf=KOLEJKA_CONF,
            kolejka_callback_url_prefix='http://127.0.0.1/',
            logger=self.logger
        )
        task_submit = self.data_master.new_task_submit(task_submit_id="submit_id",
                                                       package_path=self.package_path,
                                                       commit_id="1",
                                                       submit_path=self.submit_path)
        asyncio.run(task_submit.initialise())
        asyncio.run(self.package_manager.build_package(task_submit.package))
        set_submit = task_submit.set_submits[0]
        asyncio.run(kolejka_messanger.send(set_submit))
        self.assertEqual(set_submit.get_result().tests['1'].status, 'MEM')


if __name__ == '__main__':
    unittest.main()
