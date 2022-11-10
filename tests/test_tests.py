import pytest


def test_get_uuid():
    from tracktolib.tests import get_uuid
    assert get_uuid(1) == '00000000-0000-0000-0000-000000000001'


def test_assert_equals(capsys):
    from tracktolib.tests import assert_equals
    assert_equals({'foo': 1}, {'foo': 1})
    with pytest.raises(AssertionError):
        assert_equals({'foo': 1}, {'foo': 2})
    assert capsys.readouterr().out == '{\'values_changed\': {"root[\'foo\']": {\'new_value\': 2, \'old_value\': 1}}}\n'
