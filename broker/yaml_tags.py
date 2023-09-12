from pathlib import Path
import yaml


# class Include(yaml.YAMLObject):
#     yaml_tag = u'!include'
#
#     def __init__(self) -> None:
#         pass
#
#     def __str__(self):
#         return ''
#
#     def __repr__(self):
#         return f'include'
#
#     @classmethod
#     def to_yaml(cls, dumper, data):
#         return dumper.represent_scalar(cls.yaml_tag, )


class File:
    def __init__(self, path: str | Path) -> None:
        self.path = path
        if isinstance(path, Path):
            self.path = path.name

    def __repr__(self):
        return self.path


def file_representer(dumper: yaml.SafeDumper, file: File):
    return dumper.represent_scalar('!file', file.path)


def get_dumper():
    dumper = yaml.SafeDumper
    # dumper.add_representer(Include, Include.to_yaml)
    dumper.add_representer(File, file_representer)
    return dumper
