import os
from pathlib import Path

from baca2PackageManager import Package, TSet
from yaml import dump

from settings import BUILD_NAMESPACE, KOLEJKA_SRC_DIR


class Builder:
    def __init__(self, package: Package, enable_shortcut: bool = True) -> None:
        self.package = package
        self.build_namespace = BUILD_NAMESPACE
        self.build_path = None
        self.enable_shortcut = enable_shortcut
        self.common_path = None

    @property
    def is_built(self) -> bool:
        return self.package.check_build(self.build_namespace)

    @staticmethod
    def to_yaml(data: dict, path: Path) -> None:
        with open(path, mode='wt', encoding='utf-8') as file:
            dump(data, file)


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
        if self.enable_shortcut:
            test_yaml['satori'] = {
                'result': {
                    'execute_time_real': '/io/executor/run/real_time',
                    'execute_time_cpu': '/io/executor/run/cpu_time',
                    'execute_memory': '/io/executor/run/memory',
                    'compile_log': 'str:/builder/**/stdout,/builder/**/stderr',
                    'tool_log': 'str:/io/generator/**/stderr,/io/verifier/**/stdout,/io/verifier/**/stderr,/io/hinter/**/stderr',
                    'checker_log': 'str:/io/checker/**/stdout,/io/checker/**/stderr',
                    'logs': '/logs/logs',
                    'debug': '/debug/debug',
                }
            }

        return test_yaml

    def _create_common(self,
                       test_yaml: dict,
                       ):
        self.common_path = self.build_path / 'common'
        self.common_path.mkdir()

        self.to_yaml(test_yaml, self.common_path / 'test.yaml')

        os.symlink(KOLEJKA_SRC_DIR / 'kolejka-judge', self.common_path / 'kolejka-judge')
        os.symlink(KOLEJKA_SRC_DIR / 'kolejka-client', self.common_path / 'kolejka-client')
        # TODO: Add judge.py to common directory



    def build(self):
        self.build_path = self.package.prepare_build(self.build_namespace)

        test_yaml = self._generate_test_yaml()
        self._create_common(test_yaml)

class SetBuilder:
    def __init__(self, package: Package, t_set: TSet) -> None:
        self.package = package
        self.t_set = t_set

    def build(self):
        pass
