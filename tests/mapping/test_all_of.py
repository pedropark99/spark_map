from spark_map.mapping import all_of
import pytest


# Test if all_of() returns a dict
def test_type_return():
    result = all_of(['a', 'b', 'c'])
    assert isinstance(result, dict)

# Test if the 'val' item is a list
def test_type_return2():
    result = all_of(['a', 'b', 'c'])
    assert isinstance(result['val'], list)




# Empty lists and "None" inputs should raise an error
def test_empty_list():
    with pytest.raises(ValueError) as info:
        all_of([])

def test_none_values():
    with pytest.raises(TypeError) as info:
        all_of(None)


