import os
from pathlib import Path

from baca2PackageManager import Package, TSet, TestF
from yaml import dump
import settings

from .yaml_tags import get_dumper, File

INCLUDE_TAG = '0tag::include'


class Builder:
    TRANSLATE_CMD = {
        'test_generator': 'generator',
        'memory_limit': 'memory',
        'time_limit': 'time',
        # 'source_verifier': 'verifier',
        # 'source_name': 'basename',
    }
    IGNORED_KEYS = ['name', 'points', 'weight', 'tests']

    def __init__(self, package: Package, enable_shortcut: bool = True) -> None:
        self.package = package
        self.build_namespace = settings.BUILD_NAMESPACE
        self.build_path = None
        self.enable_shortcut = enable_shortcut
        self.common_path = None
        self.source_size = package.get('source_size')

    @property
    def is_built(self) -> bool:
        return self.package.check_build(self.build_namespace)

    @staticmethod
    def to_yaml(data: dict, path: Path) -> None:
        with open(path, mode='wt', encoding='utf-8') as file:
            file.write(dump(data, Dumper=get_dumper()))

        # replace import tags
        with open(path, mode='r', encoding='utf-8') as file:
            content = file.read()

        content = content.replace(INCLUDE_TAG, '!include ')

        with open(path, mode='wt', encoding='utf-8') as file:
            file.write(content)

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
            test_yaml['kolejka']['satori'] = {
                'result': {
                    'execute_time_real': '/io/executor/run/real_time',
                    'execute_time_cpu': '/io/executor/run/cpu_time',
                    'execute_memory': '/io/executor/run/memory',
                    'compile_log': 'str:/builder/**/stdout,/builder/**/stderr',
                    'tool_log': 'str:/io/generator/**/stderr,/io/verifier/**/stdout,'
                                '/io/verifier/**/stderr,/io/hinter/**/stderr',
                    'checker_log': 'str:/io/checker/**/stdout,/io/checker/**/stderr',
                    'answer': 'str:/io/executor/run/stdout',
                    'logs': '/logs/logs',
                    'debug': '/debug/debug',
                }
            }

        if self.package.get('checker'):
            test_yaml['checker'] = File('checker' + Path(self.package['checker']).suffix)
        if self.package.get('verifier'):
            test_yaml['verifier'] = File('verifier' + Path(self.package['verifier']).suffix)
        if self.package.get('hinter'):
            test_yaml['hinter'] = File('hinter' + Path(self.package['hinter']).suffix)
        if self.package.get('regex_count'):
            regex_count = self.package['regex_count']
            if isinstance(regex_count, list):
                regex_count = ','.join(map(str, regex_count))
            test_yaml['regex_count'] = regex_count

        return test_yaml

    def create_path_from_config(self, test_yaml, conf_name: str):
        if self.package.get(conf_name):
            conf_path = self.package.commit_path / self.package[conf_name]
            if conf_path.exists():
                os.symlink(conf_path, self.common_path / (conf_name + conf_path.suffix))
            else:
                test_yaml.pop(conf_name, None)

    def _create_common(self,
                       test_yaml: dict,
                       judge_type: str = 'main'):
        self.common_path = self.build_path / 'common'
        self.common_path.mkdir()

        os.symlink(settings.KOLEJKA_SRC_DIR / 'kolejka-judge', self.common_path / 'kolejka-judge')
        os.symlink(settings.KOLEJKA_SRC_DIR / 'kolejka-client', self.common_path / 'kolejka-client')
        os.symlink(settings.JUDGES[judge_type], self.common_path / 'judge.py')
        self.create_path_from_config(test_yaml, 'checker')
        self.create_path_from_config(test_yaml, 'verifier')
        self.create_path_from_config(test_yaml, 'hinter')

        self.to_yaml(test_yaml, self.common_path / 'test.yaml')

    def build(self):
        self.build_path = self.package.prepare_build(self.build_namespace)

        test_yaml = self._generate_test_yaml()
        self._create_common(test_yaml)

        for t_set in self.package.sets():
            set_builder = SetBuilder(self.package, t_set, self.build_path)
            set_builder.build()


class SetBuilder:
    def __init__(self, package: Package, t_set: TSet, build_path: Path) -> None:
        self.package = package
        self.t_set = t_set
        self.name = t_set['name']
        self.build_path = build_path / self.name

    def _generate_test_yaml(self):
        test_yaml = {
            INCLUDE_TAG: '../common/test.yaml',
        }
        if self.t_set.get('environment') is not None:
            env = self.t_set['environment']
            os.symlink(env, self.build_path / env)
            test_yaml['environment'] = File(env)

        for k, v in self.t_set:
            key = Builder.TRANSLATE_CMD.get(k, k)
            if key == 'time':
                v = f'{v * 1000}ms'
            if key not in Builder.IGNORED_KEYS + ['checker', 'verifier',
                                                  'hinter', 'environment'] and v is not None:
                test_yaml[key] = v
        return test_yaml

    def _add_test(self, test_yaml: dict, test: TestF, include_test: bool = True):
        single_test = {}
        if include_test:
            single_test[INCLUDE_TAG] = 'test.yaml'

        if test.get('input') is not None:
            test_filename = test['name'] + '.in'
            os.symlink(test['input'], self.build_path / test_filename)
            single_test['input'] = File(test_filename)
        if test.get('output') is not None:
            test_filename = test['name'] + '.out'
            os.symlink(test['output'], self.build_path / test_filename)
            single_test['hint'] = File(test_filename)

        for k, v in test:
            key = Builder.TRANSLATE_CMD.get(k, k)
            if key == 'time':
                v = f'{v * 1000}ms'
            if key not in Builder.IGNORED_KEYS[:] + ['input', 'output', 'hint', 'checker',
                                                     'verifier', 'hinter']:
                single_test[key] = v

        test_yaml[test['name']] = single_test

    def build(self):
        os.mkdir(self.build_path)

        test_yaml = self._generate_test_yaml()

        tests_yaml = {}
        for test in self.t_set.tests():
            self._add_test(tests_yaml, test)

        Builder.to_yaml(test_yaml, self.build_path / 'test.yaml')
        Builder.to_yaml(tests_yaml, self.build_path / 'tests.yaml')
