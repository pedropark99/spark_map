import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, sum, count, cast
from pyspark.sql.column import Column

from spark_map.functions import spark_map, spark_across
from spark_map.mapping import (
    all_of
    , matches
    , starts_with
    , ends_with
    , at_position
    , are_of_type
)

spark = SparkSession.builder.getOrCreate()

students = spark.read.parquet("data/students.parquet")
transf = spark.read.parquet("data/transf.parquet")
stock_prices = spark.read.parquet("data/stock-prices-dija.parquet")
mpg = spark.read.parquet("data/mpg.parquet")


def test_spark_map_at_position():
    spark_map(students, at_position(2, 3, 4), mean)
    spark_map(students, at_position(2), mean)
    spark_map(stock_prices, at_position(3, 2), mean)

def test_spark_map_matches():
    spark_map(students, matches("^Score[12]"), sum)
    spark_map(transf, matches("^destination"), count)


def test_spark_map_are_of_type():
    spark_map(transf, are_of_type("int"), count)
    spark_map(stock_prices, are_of_type("double"), mean)
    spark_map(transf, are_of_type("string"), count)
    spark_map(students, are_of_type("long"), sum)
    spark_map(transf, are_of_type("datetime"), count)
    spark_map(transf, are_of_type("date"), count)


def test_spark_map_starts_with():
    spark_map(students, starts_with("Score"), sum)
    spark_map(transf, starts_with("destination"), sum)


def test_spark_map_ends_with():
    spark_map(stock_prices, ends_with("e"), count)
    spark_map(transf, ends_with("ID"), count)

def test_spark_map_all_of():
    candidates = [
        'a', 'b', 'c',
        'Score1', 'Score2',
        'Open', 'Close'
    ]
    spark_map(students, all_of(candidates), sum)
    spark_map(stock_prices, all_of(candidates), mean)


def add_number_to_column(col: Column, num: int = 1):
    return col + num

def test_spark_across_are_of_type():
    spark_across(students, are_of_type("long"), add_number_to_column, num = 5)





def test_mapping_at_position_error():
    # The `students` dataset have 10 columns.
    # So it should not find a fourteenth and fifteenth columns:
    with pytest.raises(IndexError) as info:
        spark_map(students, at_position(14, 15), mean)

# Testing regexes that does not find any column
def test_mapping_matches_error():
    # The regex "Score$" should not find any column in `students` dataset.
    # As a result, it should return a KeyError.
    with pytest.raises(KeyError) as info:
        spark_map(students, matches("Score$"), sum)

def test_mapping_starts_with_error():
    # `starts_with()` and `ends_with()` are case-sensitive. As a consequence,
    # it should not find any column in the two examples below:
    with pytest.raises(KeyError) as info:
        spark_map(students, starts_with("score"), sum)
    with pytest.raises(KeyError) as info:
        spark_map(transf, starts_with("Destination"), count)

def test_mapping_ends_with_error():
    # `starts_with()` and `ends_with()` are case-sensitive. As a consequence,
    # it should not find any column in the two examples below:
    with pytest.raises(KeyError) as info:
        spark_map(stock_prices, ends_with("E"), count)
    with pytest.raises(KeyError) as info:
        spark_map(transf, ends_with("id"), count)

def test_mapping_are_of_type_error():
    # `stock_prices` have no column of type `LongType()`.
    # As a consequence, the code below should not find any column:
    with pytest.raises(KeyError) as info:
        spark_map(stock_prices, are_of_type("long"), count)

def test_mapping_all_of_error():
    # `stock_prices` have no columns named "a" or "b".
    # As a consequence, the code below should not find any column:
    with pytest.raises(KeyError) as info:
        spark_map(stock_prices, all_of(["a", "b"]), count)