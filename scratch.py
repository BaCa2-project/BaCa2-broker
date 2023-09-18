from pathlib import Path
from settings import *

from broker.submit import *
from broker.master import *

pkg_path = BASE_DIR / 'tests' / 'test_packages' / '1'
submit_path = pkg_path / '1' / 'prog' / 'solution.cpp'

master = BrokerMaster(DB_STRING, SUBMITS_DIR, DELETE_RECORDS)
master.connection.truncate_db()

master.new_submit(
    '1',
    pkg_path,
    '1',
    submit_path
)
