# Settings for broker
import os
from pathlib import Path
from datetime import timedelta
from dotenv import load_dotenv

from baca2PackageManager import set_base_dir, add_supported_extensions

MODES = {
    'production': {
        'delete_records': False,
        'verbose': True,
        'force_rebuild': True,
        'server_ip': 'baca2.ii.uj.edu.pl',
        'server_port': 8180,
        'default_timeout': timedelta(minutes=10),
        'default_timestep': timedelta(seconds=10),
        'active_wait': False,
    },
    'development': {
        'delete_records': True,
        'verbose': True,
        'force_rebuild': True,
        'default_timeout': timedelta(minutes=3),
        'default_timestep': timedelta(seconds=2),
        'active_wait': True,
        'server_ip': '127.0.0.1',
        'server_port': 8180,
    }
}
APP_MODE = os.getenv('APP_MODE', 'development')
APP_SETTINGS = MODES[APP_MODE]

BASE_DIR = Path(__file__).resolve().parent
_baca2_dir_in = os.getenv('BACA2_DIR')
if _baca2_dir_in is not None:
    BACA2_DIR = Path(_baca2_dir_in)
else:
    BACA2_DIR = BASE_DIR.parent.parent / 'BaCa2'  # Change if you have a different path
_packages_dir_in = os.getenv('PACKAGES_DIR')
if _packages_dir_in is not None:
    PACKAGES_DIR = Path(_packages_dir_in)
else:
    PACKAGES_DIR = BACA2_DIR / 'packages_source'
_submits_dir_in = os.getenv('SUBMITS_DIR')
if _submits_dir_in is not None:
    SUBMITS_DIR = Path(_submits_dir_in)
else:
    SUBMITS_DIR = BASE_DIR / 'submits'

KOLEJKA_SRC_DIR = BASE_DIR / 'kolejka_src'
JUDGES_SRC_DIR = BASE_DIR / 'judges'
KOLEJKA_CONF = BASE_DIR / 'kolejka.conf'

JUDGES = {
    'main': JUDGES_SRC_DIR / 'judge_main.py'
}

DB_STRING = f"{BASE_DIR.absolute() / 'submit_control.db'}"
# DB_STRING = f"sqlite://submit_control.db"

BUILD_NAMESPACE = 'kolejka'

set_base_dir(PACKAGES_DIR)
add_supported_extensions('cpp')

BACA_URL = os.getenv('BACA_URL')
# Number of tries to send a submit results back to BaCa2
BACA_SEND_TRIES = 1
# Interval between tries to send a submit results back to BaCa2
BACA_SEND_INTERVAL = 0.4

# Where results should be sent back to BaCa2
BACA_RESULTS_URL = f'{BACA_URL}/result'
# Where error notifications should be sent to BaCa2
BACA_ERROR_URL = f'{BACA_URL}/error'

# Passwords for protecting communication channels between the broker and BaCa2.
# PASSWORDS HAVE TO DIFFERENT IN ORDER TO BE EFFECTIVE
BACA_PASSWORD = os.getenv('BACA_PASSWORD')
BROKER_PASSWORD = os.getenv('BROKER_PASSWORD')

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
