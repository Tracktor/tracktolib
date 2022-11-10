import subprocess
import itertools
from typing import Iterable, TypeVar, Iterator

T = TypeVar('T')


def exec_cmd(cmd: str | list[str]) -> str:
    stdout, stderr = subprocess.Popen(cmd,
                                      shell=True,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE).communicate()
    if stderr:
        raise Exception(stderr.decode('utf-8'))
    return stdout.decode('utf-8')


def get_chunks(it: Iterable[T], size: int,
               *,
               as_list: bool = False) -> Iterator[Iterable[T]]:
    iterator = iter(it)
    for first in iterator:
        d = itertools.chain([first], itertools.islice(iterator, size - 1))
        yield d if not as_list else list(d)
