import pytest
import re

from spark_map.mapping import matches


def test_empty_string():
    with pytest.raises(ValueError) as info:
        matches('')

def test_none_values():
    with pytest.raises(TypeError) as info:
        matches(None)

