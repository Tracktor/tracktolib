import datetime as dt
import itertools
import os
import subprocess
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import Iterable, TypeVar, Iterator, Literal, overload
import mmap
from pathlib import Path

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


@overload
def get_chunks(it: Iterable[T], size: int,
               *,
               as_list: Literal[False]) -> Iterator[Iterable[T]]: ...


@overload
def get_chunks(it: Iterable[T], size: int,
               *,
               as_list: Literal[True]) -> Iterator[list[T]]: ...


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
    if isinstance(obj, (IPv4Address, IPv6Address)):
        return str(obj)
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Type '{type(obj)}' not serializable")


def get_nb_lines(file: Path) -> int:
    """
    Source: https://stackoverflow.com/a/68385697/2265812

    """
    with file.open("r+") as f:
        buf = mmap.mmap(f.fileno(), 0)

    nb_lines = 0
    while buf.readline():
        nb_lines += 1
    return nb_lines
