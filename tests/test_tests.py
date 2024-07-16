import pytest


def test_assert_equals(capsys):
    from tracktolib.tests import assert_equals

    assert_equals({"foo": 1}, {"foo": 1})
    assert_equals([{"foo": 1}, {"bar": 1}], [{"bar": 1}, {"foo": 1}], ignore_order=True)
    with pytest.raises(AssertionError):
        assert_equals({"foo": 1}, {"foo": 2})
    assert capsys.readouterr().out == "{'values_changed': {\"root['foo']\": {'new_value': 2, 'old_value': 1}}}\n"
