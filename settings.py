# Settings for broker
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_STRING = f"sqlite://{BASE_DIR.absolute()}/submit_control.db"

DELETE_RECORDS = False



