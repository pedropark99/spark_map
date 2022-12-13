# Databricks notebook source
# DBTITLE 1,Libraries and imports
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, GroupedData
from pyspark.sql.types import *
import re

from mapping import Mapping

# REPRODUCIBILITY WARNING:
# This source is developed inside Databricks platform, as a Databricks notebook. In every 
# session of a Databricks notebook, the platform automaticaly allocate a variable called `spark`
# which holds all the information of the Spark Session. Having this information in mind,
# you may have problems regarding the Spark Session definition, while running this source.
# If such problem happen, try to define a variable `spark` with your Spark Session, like this:
# ```
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
# ```

# COMMAND ----------

# DBTITLE 1,The `spark_map()` function

def spark_map(table, mapping, function):
  """
  With `spark_map()`, you can apply a function to a set of columns in a spark DataFrame.
  It receives a spark DataFrame as input, and returns a new aggregated spark DataFrame as output.
  For example, to apply the `mean()` function, to the third, fourth and fifth columns, you do:
  
  ```
  import pyspark.sql.functions as F
  tb = spark.table('lima.vweventtracks')
  spark_map(tb, at_position(3,4,5), F.mean)
  ```
  """
  if isinstance(table, GroupedData):
    cols = table._df.columns
    schema = list(table._df.schema)
  if isinstance(table, DataFrame):
    cols = table.columns
    schema = list(table.schema)
  
  mapping = build_mapping(mapping, cols, schema)
  message = f"Selected columns by `spark_map()`: {', '.join(mapping)}\n"
  print(message)
  
  params = []
  for col in mapping:
    params.append(function(F.col(col)).alias(col))
    
  result = table.agg(*params)
  
  return result

# COMMAND ----------

# DBTITLE 1,The `spark_across()` function
def spark_across(table, mapping, function, **kwargs):
  
  if isinstance(table, GroupedData):
    cols = table._df.columns
    schema = list(table._df.schema)
  if isinstance(table, DataFrame):
    cols = table.columns
    schema = list(table.schema)
  
  mapping = build_mapping(mapping, cols, schema)
  message = f"Selected columns by `spark_across()`: {', '.join(mapping)}\n"
  print(message)
  for col in mapping:
    table = table.withColumn(col, function(col, **kwargs))
  
  return table

# COMMAND ----------

# DBTITLE 1,Functions used to define the column mapping


def check_string_type(x, mapping_function: str):
  if isinstance(x, str):
    return(True)
  else:
    raise TypeError(f"Input of `{mapping_function}` needs to be a string (data of type `str`). Not a {type(x)}.")
      
    
    
    
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
  return {'fun': "__starts_with", 'val': text}

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
  
  


  
  
  
  
def build_mapping(mapping, cols: list, schema: StructType):
  mapping_function = mapping['fun']
  value = mapping['val']
  if isinstance(mapping_function, str):
    ### If mapping['fun'] is a string, is 
    ### very likely a name for a default mapping method
    ### so lets look for it, inside the methods of Mapping class
    print("Looking for default mapping method inside Mapping class")
    m = Mapping()
    method_to_call = getattr(m, mapping_function)
    method_to_call(value, tb.columns, tb.schema)
    selected_cols = m.mapped_cols
  else:
    ### If is not a string, a function is expected
    ### instead
    selected_cols = mapping_function(value, tb.columns, tb.schema)
  
  if len(selected_cols) == 0:
    message = "`spark_map()` did not found any column that matches your mapping!"
    raise KeyError(message)
  
  return selected_cols


