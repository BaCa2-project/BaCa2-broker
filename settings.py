# Settings for broker
from pathlib import Path
from baca2PackageManager import set_base_dir, add_supported_extensions

BASE_DIR = Path(__file__).resolve().parent
BACA2_DIR = BASE_DIR.parent.parent / 'BaCa2'  # Change if you have a different path
PACKAGES_DIR = BACA2_DIR / 'packages_source'

DB_STRING = f"sqlite://{BASE_DIR.absolute()}/submit_control.db"

DELETE_RECORDS = False

BUILD_NAMESPACE = 'kolejka'

set_base_dir(PACKAGES_DIR)
add_supported_extensions('cpp')
