from baca2PackageManager import Package
from settings import BUILD_NAMESPACE


class Builder:
    def __init__(self, package: Package) -> None:
        self.package = package
        self.build_namespace = BUILD_NAMESPACE
        self.build_path = None

    @property
    def is_built(self) -> bool:
        return self.package.check_build(self.build_namespace)

    def _generate_test_yaml(self):
        test_yaml = {
            'memory': '512MB',
            'kolejka': {
                'image': 'kolejka/satori:judge',
                'exclusive': False,
                'requires': ['cpu:xeon e3-1270 v5'],
                'collect': ['log.zip'],
                'limits': {
                    'time': '600s',
                    'memory': '10G',
                    'swap': 0,
                    'cpus': self.package['cpus'],
                    'network': self.package['network'],
                    'storage': '5G',
                    'workspace': '5G',
                }
            }
        }

        return test_yaml

    def build(self):
        self.build_path = self.package.prepare_build(self.build_namespace)

        test_yaml = self._generate_test_yaml()

        # TODO


