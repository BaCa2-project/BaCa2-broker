from unittest import TestCase
from broker.builder import *
from baca2PackageManager import *
from settings import BASE_DIR

set_base_dir(BASE_DIR / 'tests' / 'test_packages')
add_supported_extensions('cpp')


class TestBuilder(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.path = BASE_DIR / 'tests' / 'test_packages'

    def test_build_auto_pass(self):
        pkg = Package(self.path / '1', '1')
        builder = Builder(pkg)
        builder.build()
        self.assertTrue(builder.is_built)
