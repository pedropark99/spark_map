import pytest

from spark_map.mapping import at_position


def test_at_position():
    at_position(1,2,3)
    at_position(44)
    at_position(8,2,5,4)



def test_order():
    # Input values should be sorted in the output
    result = at_position(8,2,5,4, zero_index = True)
    assert result['val'] == [2,4,5,8]

# Empty lists and None values should raise an error:
def test_empty_list():
    with pytest.raises(Exception) as info:
        at_position([])

def test_none_value():
    with pytest.raises(Exception) as info:
        at_position(None)

def test_string_values():
    with pytest.raises(Exception) as info:
        at_position("1", "2", "3")

def test_double_values():
    with pytest.raises(Exception) as info:
        at_position(1.25, 3.4, 6.7)