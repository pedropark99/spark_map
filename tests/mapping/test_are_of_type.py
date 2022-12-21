from spark_map.mapping import are_of_type
import pytest


def test_empty_strings():
    with pytest.raises(ValueError) as info:
        are_of_type("")


def test_none_values():
    with pytest.raises(TypeError) as info:
        are_of_type(None)



def test_valid_data_types():
    valid_types = ['string', 'int', 'long', 'double', 'date', 'datetime']
    for type in valid_types:
        are_of_type(type)

def test_invalid_data_types():
    with pytest.raises(ValueError) as info:
        are_of_type("weird data type")

    with pytest.raises(ValueError) as info:
        are_of_type("str")

    with pytest.raises(ValueError) as info:
        are_of_type("long description")