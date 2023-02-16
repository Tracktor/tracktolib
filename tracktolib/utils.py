import datetime as dt
import itertools
import mmap
import os
import subprocess
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import Iterable, TypeVar, Iterator, Literal, overload, Any
import importlib.util

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


def import_module(path: Path):
    """
    Import a module from a path.
    Eg:
        >>> from tracktolib.utils import import_module
        >>> module = import_module(Path('~/my_module.py'))
        >>> module.my_function()
    """
    name = path.name.removesuffix('.py')
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None:
        raise ImportError(f'Could not import {path}')
    module = importlib.util.module_from_spec(spec)
    if spec.loader is not None:
        spec.loader.exec_module(module)
    return module


@overload
def get_chunks(it: Iterable[T], size: int,
               *,
               as_list: Literal[False]) -> Iterator[Iterable[T]]: ...


@overload
def get_chunks(it: Iterable[T], size: int,
               *,
               as_list: Literal[True]) -> Iterator[list[T]]: ...


@overload
def get_chunks(it: Iterable[T], size: int) -> Iterator[list[T]]: ...


def get_chunks(it: Iterable[T], size: int,
               *,
               as_list: bool = True) -> Iterator[Iterable[T]]:
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


def fill_dict(items: list[dict],
              *,
              keys: list | None = None,
              default: Any | None = None) -> list[dict]:
    """Returns a list of items with the same key for all"""

    def _fill_dict(x):
        return {k: x.get(k, default) for k in _keys}

    _keys = keys or sorted(frozenset().union(*items))
    return [_fill_dict(x) for x in items]


def to_snake_case(string: str) -> str:
    return ''.join(['_' + i.lower() if i.isupper() else i for i in string]).lstrip('_')


def to_camel_case(string: str) -> str:
    return ''.join(word.capitalize() if i > 0 else word for i, word in enumerate(string.split('_')))


@overload
def dict_to_camel(d: dict) -> dict: ...


@overload
def dict_to_camel(d: list[dict]) -> list[dict]: ...


def dict_to_camel(d: dict | list):
    """
    Convert all keys of a dict or list of dicts to camel case
    """

    def _parse_item(v):
        return dict_to_camel(v) if isinstance(v, dict) else v

    def _parse_list(dl):
        return [_parse_item(v) for v in dl]

    if isinstance(d, list):
        return _parse_list(d)

    return {to_camel_case(k): _parse_list(v) if isinstance(v, list) else _parse_item(v) for k, v in d.items()}


@overload
def rm_keys(data: dict, keys: list[str]) -> dict: ...


@overload
def rm_keys(data: list[dict], keys: list[str]) -> list[dict]: ...


def rm_keys(data: dict | list[dict], keys: list[str]):
    """Remove keys from a dict or a list of dicts"""
    _data = data if isinstance(data, list) else [data]
    for d in _data:
        for key in keys:
            assert d.pop(key, None) is not None
    return _data if isinstance(data, list) else _data[0]


def num_not_none(*args) -> int:
    """
    Count the number of non None arguments
    """
    return sum(1 for x in args if x is not None)
