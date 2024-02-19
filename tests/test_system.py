import unittest
import concurrent.futures as cf
import threading
from time import sleep

from baca2PackageManager.broker_communication import BacaToBroker, make_hash
import requests
import uvicorn

import settings
from app.main import app


class SystemTest(unittest.TestCase):

    resource_dir = settings.BASE_DIR / 'tests' / 'resources'

    def setUp(self):
        self.server_thread = threading.Thread(
            target=uvicorn.run,
            args=(app,),
            kwargs={'host': settings.SERVER_HOST, 'port': settings.SERVER_PORT},
        )
        self.server_thread.start()
        sleep(1)
        self.package_path = self.resource_dir / '1'
        self.submit_path = self.resource_dir / '1' / '1' / 'prog' / 'solution.cpp'

    def test(self):
        futures: list[cf.Future] = []
        with cf.ThreadPoolExecutor() as executor:
            with requests.session() as s:
                for i in range(10):
                    submit_id = str(i)
                    btb = BacaToBroker(make_hash(settings.BROKER_PASSWORD, submit_id),
                                       submit_id,
                                       str(self.package_path),
                                       '1',
                                       str(self.submit_path))
                    tmp = executor.submit(s.post,
                                          f'http://{settings.SERVER_HOST}:{settings.SERVER_PORT}/baca',
                                          json=btb.serialize())
                    futures.append(tmp)
                cf.wait(futures)
                for f in futures:
                    self.assertEqual(f.result().status_code, 200)
            sleep(10)


if __name__ == '__main__':
    unittest.main()
