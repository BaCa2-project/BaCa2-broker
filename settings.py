# Settings for broker
from pathlib import Path
from baca2PackageManager import set_base_dir, add_supported_extensions

MODES = {
    'production': {
        'delete_records': False,
        'verbose': False,
        'force_rebuild': False,
        'server_ip': 'http://baca2.ii.uj.edu.pl',
        'server_port': 9012
    },
    'development': {
        'delete_records': True,
        'verbose': True,
        'force_rebuild': True,
        'server_ip': '127.0.0.1',
        'server_port': 9014
    }
}
APP_MODE = 'development'
APP_SETTINGS = MODES[APP_MODE]

BASE_DIR = Path(__file__).resolve().parent
BACA2_DIR = BASE_DIR.parent.parent / 'BaCa2'  # Change if you have a different path
PACKAGES_DIR = BACA2_DIR / 'packages_source'
KOLEJKA_SRC_DIR = BASE_DIR / 'kolejka_src'
JUDGES_SRC_DIR = BASE_DIR / 'judges'
SUBMITS_DIR = BASE_DIR / 'submits'
KOLEJKA_CONF = BASE_DIR / 'kolejka.conf'

JUDGES = {
    'main': JUDGES_SRC_DIR / 'judge_main.py'
}

DB_STRING = f"{BASE_DIR.absolute() / 'submit_control.db'}"
# DB_STRING = f"sqlite://submit_control.db"

DELETE_RECORDS = False

BUILD_NAMESPACE = 'kolejka'

set_base_dir(PACKAGES_DIR)
add_supported_extensions('cpp')
