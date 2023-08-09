from unittest import TestCase
from broker.builder import *
from baca2PackageManager import *
from settings import BASE_DIR

set_base_dir(BASE_DIR / 'tests' / 'test_packages')
add_supported_extensions('cpp')

# class TestBuilder(TestCase):
#     pass
