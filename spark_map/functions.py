from pyspark.sql.functions import column
from pyspark.sql import DataFrame, GroupedData
from pyspark.sql.types import *
import re

from spark_map.mapping import build_mapping, check_string_type



def spark_map(table, mapping, function):
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
  cols = __get_columns(table)
  schema = __get_schema(table)
  
  mapping = build_mapping(mapping, cols, schema)
  message = f"Selected columns by `spark_map()`: {', '.join(mapping)}\n"
  print(message)
  
  params = []
  for col in mapping:
    params.append(function(column(col)).alias(col))
    
  result = table.agg(*params)
  
  return result





def spark_across(table:DataFrame, mapping, function, **kwargs):
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
  if isinstance(table, GroupedData):
    raise ValueError("You gave a grouped Spark DataFrame to `spark_across()`. However, this function work solely with plain Spark DataFrames. Did you meant to use `spark_map()` instead?")
  
  cols = table.columns
  schema = table.schema
  
  mapping = build_mapping(mapping, cols, schema)
  message = f"Selected columns by `spark_across()`: {', '.join(mapping)}\n"
  print(message)
  for col in mapping:
    table = table.withColumn(col, function(col, **kwargs))
  
  return table




def __get_columns(table:DataFrame):
  if isinstance(table, GroupedData):
    cols = table._df.columns
  if isinstance(table, DataFrame):
    cols = table.columns

  return cols


def __get_schema(table:DataFrame):
  if isinstance(table, GroupedData):
    schema = list(table._df.schema)
  if isinstance(table, DataFrame):
    schema = list(table.schema)

  return schema




    
    
    
def all_of(list_cols: list):
  return {'fun': 'all_of', 'val': list_cols}
    
def matches(regex: str):
  check_string_type(regex, "matches()")
  return {'fun': "matches", 'val': regex} 

  
def at_position(*indexes, zero_index = False):
  if len(indexes) == 0:
    raise ValueError("You did not provided any index for `at_position()` to search")
  if isinstance(indexes[0], list):
    raise ValueError("Did you provided your column indexes inside a list? You should not encapsulate these indexes inside a list. For example, if you want to select 1° and 3° columns, just do `at_position(1, 3)` instead of `at_position([1, 3])`.")  
  if zero_index == False:
    indexes = [index - 1 for index in indexes]
  
  # Check if any of the indexes are negative:
  negative = [index < 0 for index in indexes]
  if any(negative):
    raise ValueError("One (or more) of the provided indexes are negative! Did you provided a zero index, and not set the `zero_index` argument to True?")
    
  # Transform to `set` to avoid duplicates indexes
  indexes = tuple(set(indexes))
  return {'fun': "at_position", 'val': indexes}


def starts_with(text: str):
  check_string_type(text, "starts_with()")
  return {'fun': "starts_with", 'val': text}

def ends_with(text: str):
  check_string_type(text, "ends_with()")
  return {'fun': "ends_with", 'val': text}

def are_of_type(arg_type: str):
  check_string_type(arg_type, "are_of_type()")
  valid_types = ['string', 'int', 'double', 'date', 'datetime']
  if arg_type not in valid_types:
    types = [f"'{t}'" for t in valid_types]
    types = ', '.join(types)
    message = f'You must choose one of the following values: {types}'
    raise ValueError(message)
  return {'fun': "are_of_type", 'val': arg_type}

