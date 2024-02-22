from unittest import TestCase

from baca2PackageManager import *

from app.broker.builder import Builder

set_base_dir(Path(__file__).parent.parent / 'resources')
add_supported_extensions('cpp')


class TestBuilder(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.path = Path(__file__).parent.parent / 'resources'

    def test_build_auto_pass(self):
        pkg = Package(self.path / '1', '1')
        builder = Builder(pkg)
        builder.build()
        self.assertTrue(builder.is_built)
