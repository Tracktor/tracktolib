import pprint
from typing import Iterable

try:
    import deepdiff
except ImportError:
    raise ImportError('Please install tracktolib with "tests" to use this module')


def get_uuid(i: int = 0):
    return f'00000000-0000-0000-0000-000000{i:06}'


def assert_equals(d1: dict | Iterable[dict], d2: dict | Iterable[dict]):
    assert d1 == d2, pprint.pprint(deepdiff.DeepDiff(d1, d2))
