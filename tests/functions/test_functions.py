import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, sum, count

from spark_map.functions import spark_map
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
