import pprint
from typing import Iterable
import warnings


try:
    import deepdiff
except ImportError:
    raise ImportError('Please install deepdiff or tracktolib with "tests" to use this module')


def get_uuid(i: int = 0):
    warnings.warn("Please use uuid.UUID(int=i) instead", DeprecationWarning)
    return f'00000000-0000-0000-0000-000000{i:06}'


def assert_equals(d1: dict | Iterable, d2: dict | Iterable):
    assert d1 == d2, pprint.pprint(deepdiff.DeepDiff(d1, d2))
