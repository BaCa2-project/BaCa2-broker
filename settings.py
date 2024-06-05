"""Settings for broker"""
import os
from pathlib import Path
from datetime import timedelta
from dotenv import load_dotenv

from baca2PackageManager import set_base_dir, add_supported_extensions

load_dotenv()


class ConfigError(Exception):
    pass


# Server settings
SERVER_HOST: str = os.getenv('SERVER_HOST')
SERVER_PORT: int = int(os.getenv('SERVER_PORT'))
SERVER_URL: str = os.getenv('SERVER_URL')

ACTIVE_WAIT: bool = os.getenv('ACTIVE_WAIT', False) == 'true'

# Path settings
BASE_DIR = Path(__file__).resolve().parent

_packages_dir_in = os.getenv('PACKAGES_DIR')
if _packages_dir_in:
    PACKAGES_DIR = Path(_packages_dir_in).resolve()
else:
    raise ConfigError('PACKAGES_DIR not set. Please set packages directory in .env file.')

_submits_dir_in = os.getenv('SUBMITS_DIR')
if _submits_dir_in:
    SUBMITS_DIR = Path(_submits_dir_in).resolve()
else:
    SUBMITS_DIR = BASE_DIR / 'submits'

KOLEJKA_SRC_DIR = BASE_DIR / 'kolejka_src'
JUDGES_SRC_DIR = BASE_DIR / 'judges'
KOLEJKA_CONF = BASE_DIR / 'kolejka.conf'

set_base_dir(PACKAGES_DIR)
add_supported_extensions('cpp')

# Judge settings
JUDGES = {
    'main': JUDGES_SRC_DIR / 'judge_main.py'
}

# Kolejka settings
KOLEJKA_CALLBACK_URL_PREFIX = f'https://{SERVER_URL}/kolejka'
BUILD_NAMESPACE = 'kolejka'

# Timeout settings
TASK_SUBMIT_TIMEOUT: timedelta = timedelta(minutes=10)
DELETION_DAEMON_INTERVAL: timedelta = timedelta(minutes=5)

# Kolejka refresh
KOLEJKA_REFRESH_INTERVAL: timedelta = timedelta(minutes=1)

# Package settings
FORCE_REBUILD_PACKAGE = False

# BaCa2 URL settings
BACA_URL = os.getenv('BACA_URL')
if not BACA_URL:
    raise ConfigError('BACA_URL not set. Please set BaCa2 URL in .env file.')
# Where results should be sent back to BaCa2
BACA_RESULTS_URL = f'{BACA_URL}/result'
# Where error notifications should be sent to BaCa2
BACA_ERROR_URL = f'{BACA_URL}/error'

# Password settings
# PASSWORDS HAVE TO DIFFERENT IN ORDER TO BE EFFECTIVE
BACA_PASSWORD = os.getenv('BACA_PASSWORD')
BROKER_PASSWORD = os.getenv('BROKER_PASSWORD')

# Logging settings
_log_dir_in = os.getenv('LOG_DIR')
if _log_dir_in:
    LOG_DIR = Path(_log_dir_in).resolve()
else:
    LOG_DIR = BASE_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / 'broker.log'
LOGGER_PROMPT = '%(asctime)s %(name)s:%(filename)s:%(lineno)d: %(message)s'
