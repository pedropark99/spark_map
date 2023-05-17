from pyspark.sql.functions import column
from pyspark.sql import DataFrame, GroupedData
from typing import Callable, List
from spark_map.mapping import __map_columns



def spark_map(table: DataFrame, mapping: dict, function: Callable):
    """
    With `spark_map()`, you can apply a aggregate function to a set of columns in a spark DataFrame.
    It receives a spark DataFrame as input, and returns a new aggregated spark DataFrame as output.
    For example, to apply the `mean()` function, to the third, fourth and fifth columns, you do: 
    
    Example
    --------
    >>> import pyspark.sql.functions as F
    >>> tb = spark.table('sales.sales_by_day')
    >>> spark_map(tb, at_position(3,4,5), F.mean)
    """
    if __is_spark_grouped_data(table):
        cols = table._df.columns
        schema = list(table._df.schema)
    if __is_spark_dataframe(table):
        cols = table.columns
        schema = list(table.schema)
    
    mapping = __map_columns(mapping, cols, schema)
    __report_mapped_columns(mapping)
    
    params = []
    for col in mapping:
        params.append(
            function(column(col)).alias(col)
        )
      
    result = table.agg(*params)
    
    return result



def spark_across(table: DataFrame, mapping: dict, function: Callable, **kwargs):
    """
    With `spark_across()` you can apply a function across multiple columns of a spark DataFrame.
    While `spark_map()` calculates aggregates in a set of columns, `spark_across()` uses
    `withColumn()` to apply a function over each row of a set of columns.

    Example
    --------
    >>> import pyspark.sql.functions as F
    >>> tb = spark.table('sales.sales_by_day')
    >>> spark_across(tb, at_position(3,4,5), F.cast, 'double')
    """
    if __is_spark_grouped_data(table):
        raise ValueError("You gave a grouped Spark DataFrame to `spark_across()`. However, this function work solely with plain Spark DataFrames. Did you meant to use `spark_map()` instead?")
    
    cols = table.columns
    schema = list(table.schema)
    
    mapping = __map_columns(mapping, cols, schema)
    __report_mapped_columns(mapping)
    
    for col in mapping:
        table = table.withColumn(
            col,
            function(column(col), **kwargs)
        )

    return table





def __is_spark_dataframe(x):
    return isinstance(x, DataFrame)

def __is_spark_grouped_data(x):
    return isinstance(x, GroupedData)


def __report_mapped_columns(mapping: List[str]):
    message = f"Selected columns by `spark_map()`: {', '.join(mapping)}\n"
    print(message)
