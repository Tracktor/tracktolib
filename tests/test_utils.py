import pytest


@pytest.mark.parametrize('cmd', [
    'ls -alh',
    ['ls', '-alh']
])
def test_exec_cmd(cmd):
    from tracktolib.utils import exec_cmd
    output = exec_cmd(cmd)
    assert output


@pytest.mark.parametrize('data', [
    range(5),
    [0, 1, 2, 3, 4]
])
def test_get_chunk(data):
    from tracktolib.utils import get_chunks

    z = get_chunks(data, size=2, as_list=True)
    assert list(z) == [[0, 1], [2, 3], [4]]
