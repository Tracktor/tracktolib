import itertools
import os
import subprocess
from typing import Iterable, TypeVar, Iterator
import datetime as dt

T = TypeVar('T')


def exec_cmd(cmd: str | list[str],
             *,
             encoding: str = 'utf-8') -> str:
    default_shell = os.getenv('SHELL', '/bin/bash')

    stdout, stderr = subprocess.Popen(cmd,
                                      shell=True,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE,
                                      executable=default_shell).communicate()
    if stderr:
        raise Exception(stderr.decode(encoding))
    return stdout.decode(encoding)


def get_chunks(it: Iterable[T], size: int,
               *,
               as_list: bool = False) -> Iterator[Iterable[T]]:
    iterator = iter(it)
    for first in iterator:
        d = itertools.chain([first], itertools.islice(iterator, size - 1))
        yield d if not as_list else list(d)


def json_serial(obj):
    """ JSON serializer for objects not serializable by default json code """
    if isinstance(obj, (dt.datetime, dt.date)):
        return obj.isoformat()
    else:
        pass

    raise TypeError(f"Type '{type(obj)}' not serializable")
