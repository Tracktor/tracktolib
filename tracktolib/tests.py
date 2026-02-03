import pprint
from typing import Iterable

try:
    import deepdiff.diff as deepdiff
except ImportError:
    raise ImportError('Please install deepdiff or tracktolib with "tests" to use this module')


def assert_equals(d1: dict | Iterable, d2: dict | Iterable, *, ignore_order: bool = False):
    diff = deepdiff.DeepDiff(d1, d2, ignore_order=ignore_order)
    assert not diff, pprint.pprint(diff)
