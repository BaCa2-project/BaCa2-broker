
from pathlib import Path
import yaml

from .message import BrokerToBaca, Test


def parse_from_kolejka(course: str, submit_id: int, directory: Path) -> BrokerToBaca:
    with open(directory / 'results' / 'results.yaml') as f:
        content: dict = yaml.load(f, Loader=yaml.Loader)
    tests: list[Test] = []
    for key in sorted(content, key=int):
        data = content[key]
        satori_info = data['satori']
        current = Test(
            status=satori_info['status'],
            time_real=satori_info['execute_time_real'],
            time_cpu=satori_info['execute_time_cpu'],
            runtime_memory=satori_info['execute_memory']
        )
        tests.append(current)
    return BrokerToBaca(
        course_name=course,
        submit_id=submit_id,
        tests=tests
    )
