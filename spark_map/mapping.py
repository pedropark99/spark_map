from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType, StringType, DateType, IntegerType, LongType, DoubleType
import re


def __is_string(x):
  return isinstance(x, str)

def __check_string_input(x, mapping_function: str):
  if __is_string(x):
    return(True)
  else:
    raise TypeError(f"Input of `{mapping_function}` needs to be a string (data of type `str`). Not {type(x)}.")




    
def all_of(list_cols: list):
  return {'fun': 'all_of', 'val': list_cols}
    
def matches(regex: str):
  __check_string_input(regex, "matches()")
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
    
  # Transform to `set` to avoid duplicated indexes
  indexes = list(set(indexes))
  return {'fun': "at_position", 'val': indexes}


def starts_with(text: str):
  __check_string_input(text, "starts_with()")
  return {'fun': "starts_with", 'val': text}

def ends_with(text: str):
  __check_string_input(text, "ends_with()")
  return {'fun': "ends_with", 'val': text}

def are_of_type(arg_type: str):
  __check_string_input(arg_type, "are_of_type()")
  valid_types = ['string', 'int', 'long', 'double', 'date', 'datetime']
  if arg_type not in valid_types:
    types = [f"'{t}'" for t in valid_types]
    types = ', '.join(types)
    message = f'You must choose one of the following values: {types}'
    raise ValueError(message)
  return {'fun': "are_of_type", 'val': arg_type}







def __map_columns(mapping, cols: list, schema: StructType):
  mapping_function = mapping['fun']
  mapping_value = mapping['val']
  if __is_string(mapping_function):
    m = Mapping()
    method_to_call = getattr(m, mapping_function)
    method_to_call(mapping_value, cols, schema)
    selected_cols = m.mapped_cols
  else:
    selected_cols = mapping_function(mapping_value, cols, schema)
  
  if len(selected_cols) == 0:
    message = "`spark_map()` did not found any column that matches your mapping!"
    raise KeyError(message)
  
  return selected_cols



    
### ====================================================================================
### We use the Mapping class to store the default mapping methods available in the package.
### If the user wants to use a custom mapping method, he should provide its own
### methods. These custom methods will not have anything in commom with this class;
### ====================================================================================
class Mapping:
    
    def __init__(self):
      self.mapped_cols = []
    
    
  
    def all_of(self, list_cols: list, cols: list, schema: StructType):
      selected_cols = [col for col in list_cols if col in cols]
      self.mapped_cols = selected_cols

    def at_position(self, indexes, cols: list, schema: StructType):
      selected_cols = [cols[i] for i in indexes]
      self.mapped_cols = selected_cols

    def ends_with(self, text: str, cols: list, schema: StructType):
      selected_cols = list()
      for col in cols:
        if col.endswith(text):
            selected_cols.append(col)

      self.mapped_cols = selected_cols

    def starts_with(self, text: str, cols: list, schema: StructType):
      selected_cols = list()
      for col in cols:
        if col.startswith(text):
            selected_cols.append(col)

      self.mapped_cols = selected_cols


    def matches(self, regex: str, cols: list, schema: StructType):
      regex = re.compile(regex)
      selected_cols = [col for col in cols if re.match(regex, col)]
      self.mapped_cols = selected_cols


    def are_of_type(self, str_type: str, cols: list, schema: StructType):
      valid_types = {
        'int' : IntegerType(), 'double' : DoubleType(), 
        'string' : StringType(), 'date' : DateType(),
        'datetime' : TimestampType(), 'long': LongType()
      }

      if str_type not in valid_types:
        valid_keys = ", ".join(list(valid_types.keys()))
        raise KeyError(f"You must choose one of the following key types: {valid_keys}")

      target_type = valid_types[str_type]
      selected_cols = list()
      for name, field in zip(cols, schema):
        if field.dataType == target_type:
          selected_cols.append(name)

      self.mapped_cols = selected_cols

