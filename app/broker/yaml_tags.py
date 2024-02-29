from pathlib import Path
import yaml


class File:
    def __init__(self, path: str | Path) -> None:
        self.path = path
        if isinstance(path, Path):
            self.path = path.name

    def __repr__(self):
        return self.path


def file_representer(dumper: yaml.SafeDumper, file: File):
    return dumper.represent_scalar('!file', file.path)


def file_constructor(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> File:
    return File(loader.construct_scalar(node))


def get_dumper():
    dumper = yaml.SafeDumper
    # dumper.add_representer(Include, Include.to_yaml)
    dumper.add_representer(File, file_representer)
    return dumper


def get_loader():
    loader = yaml.SafeLoader
    loader.add_constructor('!file', file_constructor)
    return loader
