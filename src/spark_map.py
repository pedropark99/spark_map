# Databricks notebook source
# DBTITLE 1,Libraries and imports
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, GroupedData
from pyspark.sql.types import *
import re

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

# DBTITLE 1,Functions used to define the column mapping


def check_string_type(x, mapping_function: str):
  if isinstance(x, str):
    return(True)
  else:
    raise TypeError(f"Input of `{mapping_function}` needs to be a string (data of type `str`). Not a {type(x)}.")
      
    
    
    
def all_of(list_cols: list):
  return {'fun': '__all_of', 'val': list_cols}
    
def matches(regex: str):
  check_string_type(regex, "matches()")
  return {'fun': "__matches", 'val': regex} 

  
def at_position(*indexes, zero_index = False):
  if len(indexes) == 0:
    raise ValueError("You did not provided any index for `at_position()` to search")
  if isinstance(indexes[0], list):
    raise ValueError("Did you provided your column indexes inside a list? You should not encapsulate these indexes inside a list. For example, if you want to select 1?? and 3?? columns, just do `at_position(1, 3)` instead of `at_position([1, 3])`.")  
  if zero_index == False:
    indexes = [index - 1 for index in indexes]
  
  # Check if any of the indexes are negative:
  negative = [index < 0 for index in indexes]
  if any(negative):
    raise ValueError("One (or more) of the provided indexes are negative! Did you provided a zero index, and not set the `zero_index` argument to True?")
    
  # Transform to `set` to avoid duplicates indexes
  indexes = tuple(set(indexes))
  return {'fun': "__at_position", 'val': indexes}


def starts_with(text: str):
  check_string_type(text, "starts_with()")
  return {'fun': "__starts_with", 'val': text}

def ends_with(text: str):
  check_string_type(text, "ends_with()")
  return {'fun': "__ends_with", 'val': text}

def are_of_type(arg_type: str):
  check_string_type(arg_type, "are_of_type()")
  valid_types = ['string', 'int', 'double', 'date', 'datetime']
  if arg_type not in valid_types:
    types = [f"'{t}'" for t in valid_types]
    types = ', '.join(types)
    message = f'You must choose one of the following values: {types}'
    raise ValueError(message)
  return {'fun': "__are_of_type", 'val': arg_type}
  
  
  
  
  

  
  
def __all_of(list_cols: list, cols: list, schema: StructType):
  selected_cols = [col for col in list_cols if col in cols]
  return selected_cols
  
def __at_position(indexes, cols: list, schema: StructType):
  selected_cols = [cols[i] for i in indexes]
  return selected_cols
  
def __ends_with(text: str, cols: list, schema: StructType):
  length_text = len(text)
  lengths = [len(col) for col in cols]
  
  selected_cols = list()
  for n, col in zip(lengths, cols):
    if length_text <= n:
      start = n - length_text
      end = n
      if col[start:end] == text:
        selected_cols.append(col)
  return selected_cols


def __starts_with(text: str, cols: list, schema: StructType):
  length_text = len(text)
  lengths = [len(col) for col in cols]
  
  selected_cols = list()
  for n, col in zip(lengths, cols):
    if length_text < n:
      end = 0 + length_text
      if col[0:end] == text:
        selected_cols.append(col)
    
    if (length_text == n) & (col == text):
      selected_cols.append(col)
      
  return selected_cols

  
def __matches(regex: str, cols: list, schema: StructType):
  regex = re.compile(regex)
  selected_cols = [col for col in cols if re.match(regex, col)]
  return selected_cols
  
  
def __are_of_type(str_type: str, cols: list, schema: StructType):
  valid_types = {
    'int' : IntegerType(), 'double' : DoubleType(), 
    'string' : StringType(), 'date' : DateType(), 'datetime' : TimestampType()
  }
  
  if str_type not in valid_types:
    valid_keys = ", ".join(list(valid_types.keys()))
    raise KeyError(f"You must choose one of the following key types: {valid_keys}")
    
  target_type = valid_types[str_type]
  selected_cols = list()
  for name, field in zip(cols, schema):
    if field.dataType == target_type:
      selected_cols.append(name)
      
  return selected_cols
  
  
  
  
  
  
def build_mapping(mapping, cols: list, schema: StructType):
  mapping_function = mapping['fun']
  mapping_function = globals()[mapping_function]
  
  value = mapping['val']
  selected_cols = mapping_function(value, cols, schema)
  
  if len(selected_cols) == 0:
    message = "`spark_map()` did not found any column that matches your mapping!"
    raise KeyError(message)
  
  return selected_cols



