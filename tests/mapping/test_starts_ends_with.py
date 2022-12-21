from spark_map.mapping import starts_with, ends_with
import pytest


def test_empty_string():
    # Both `starts_with()` and `ends_with()` should not receive an
    # empty string as input. As consequence, a ValueError should be raised.
    with pytest.raises(ValueError) as info:
        starts_with("")
    with pytest.raises(ValueError) as info:
        ends_with("")


def test_none_values():
    # None values are not allowed in `starts_with()` and `ends_with()`.
    with pytest.raises(TypeError) as info:
        starts_with(None)
    with pytest.raises(TypeError) as info:
        ends_with(None)


def test_values_that_are_not_strings():
    # `starts_with()` and `ends_with()` accepts only strings
    # as input. A TypeError should be raised with any input
    # that are not a string.
    with pytest.raises(TypeError) as info:
        starts_with(['test', 'abc'])
    with pytest.raises(TypeError) as info:
        ends_with(['test', 'abc'])