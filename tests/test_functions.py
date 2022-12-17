from spark_map.functions import all_of


def test_all_of_return():
    result = all_of(['a', 'b', 'c'])
    assert isinstance(result, dict)