import asyncio
import dataclasses
import datetime as dt
import decimal
import ipaddress
import json

import pytest

from tracktolib.tests import assert_equals


@pytest.mark.parametrize("cmd", ["ls -alh", ["ls", "-alh"]])
def test_exec_cmd(cmd):
    from tracktolib.utils import exec_cmd

    output = exec_cmd(cmd)
    assert output


@pytest.mark.parametrize("cmd", ["ls -alh", ["ls", "-alh"]])
def test_aexec_cmd(cmd):
    from tracktolib.utils import aexec_cmd

    output = asyncio.run(aexec_cmd(cmd))
    assert output


@pytest.mark.parametrize("data", [range(5), [0, 1, 2, 3, 4]])
def test_get_chunk(data):
    from tracktolib.utils import get_chunks

    z = get_chunks(data, size=2, as_list=True)
    assert list(z) == [[0, 1], [2, 3], [4]]
    z = get_chunks(data, size=2)
    assert list(z) == [[0, 1], [2, 3], [4]]


def test_json_serial():
    from tracktolib.utils import json_serial

    @dataclasses.dataclass
    class Custom:
        a: int
        b: dt.date

    res = json.dumps(
        {
            "dt": dt.datetime(2019, 1, 1),
            "decimal": decimal.Decimal("12.3"),
            "ipv4": ipaddress.IPv4Address("127.0.0.1"),
            "ipv6": ipaddress.IPv6Address("::1"),
            "custom": Custom(1, dt.date(2019, 1, 1)),
        },
        default=json_serial,
    )
    assert res == (
        '{"dt": "2019-01-01T00:00:00", "decimal": "12.3", "ipv4": "127.0.0.1", "ipv6": "::1", '
        '"custom": {"a": 1, "b": "2019-01-01"}}'
    )


def test_get_nb_lines(static_dir):
    from tracktolib.utils import get_nb_lines

    assert get_nb_lines(static_dir / "test.csv") == 2


def test_to_snake_case():
    from tracktolib.utils import to_snake_case

    assert to_snake_case("HelloWorld") == "hello_world"


def test_to_camel_case():
    from tracktolib.utils import to_camel_case

    assert to_camel_case("hello_world") == "helloWorld"


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"foo": 1, "bar": 2}, {"foo": 1}),
        ([{"foo": 1, "bar": 2}], [{"foo": 1}]),
    ],
)
def test_rm_keys(data, expected):
    from tracktolib.utils import rm_keys

    assert_equals(rm_keys(data, ["bar"]), expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"foo_bar": 1, "bar": 2}, {"fooBar": 1, "bar": 2}),
        ([{"foo": [{"foo_bar": {"bar_baz": "foo_bar"}}]}], [{"foo": [{"fooBar": {"barBaz": "foo_bar"}}]}]),
    ],
)
def test_dict_to_camel(data, expected):
    from tracktolib.utils import dict_to_camel

    assert_equals(dict_to_camel(data), expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        ([0, 1], 2),
        ([1, None, None], 1),
        ([None, None], 0),
    ],
)
def test_num_not_null(data, expected):
    from tracktolib.utils import num_not_none

    assert num_not_none(*data) == expected


def test_deep_reload():
    from tracktolib import pg_sync
    from tracktolib.utils import deep_reload

    deep_reload(pg_sync)


@pytest.mark.parametrize(
    ("input_chunks", "min_size", "expected_total", "extra_check"),
    [
        pytest.param(
            [b"12345", b"67890", b"abcde", b"fghij"],
            5,
            20,
            lambda chunks: all(len(c) >= 5 for c in chunks),
            id="exact_chunks",
        ),
        pytest.param(
            [b"12345678901234567890", b"abc"],
            10,
            23,
            None,
            id="small_final_chunk",
        ),
        pytest.param(
            [b"small"],
            100,
            5,
            lambda chunks: len(chunks) == 1 and chunks[0] == b"small",
            id="single_small_chunk",
        ),
        pytest.param(
            [b"hello", b"", b"world"],
            5,
            10,
            None,
            id="empty_chunks_ignored",
        ),
    ],
)
def test_get_stream_chunk(input_chunks, min_size, expected_total, extra_check):
    from tracktolib.utils import get_stream_chunk

    async def _test():
        async def async_data():
            for chunk in input_chunks:
                yield chunk

        chunks = []
        async for chunk in get_stream_chunk(async_data(), min_size=min_size):
            chunks.append(chunk)

        total_size = sum(len(c) for c in chunks)
        assert total_size == expected_total
        if extra_check:
            assert extra_check(chunks)

    asyncio.run(_test())
