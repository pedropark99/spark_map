import pyspark.sql.functions as F
from pyspark.sql import DataFrame, GroupedData
from pyspark.sql.types import *
import re




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









def check_string_type(x, mapping_function: str):
  if isinstance(x, str):
    return(True)
  else:
    raise TypeError(f"Input of `{mapping_function}` needs to be a string (data of type `str`).")
      
    
    
    
def all_of(list_cols: list):
  return {'fun': '__all_of', 'val': list_cols}
    
def matches(regex: str):
  check_string_type(regex, "matches()")
  return {'fun': "__matches", 'val': regex} 
  
def at_position(*args, zero_index = True):
  indexes = args
  if zero_index == False:
    indexes = [i - 1 for i in indexes]
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
  reg = re.compile(regex)
  selected_cols = [col for col in cols if reg.match(col)]
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








