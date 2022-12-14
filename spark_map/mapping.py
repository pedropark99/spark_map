from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType, StringType, DateType, IntegerType, LongType, DoubleType

## Helper function to check data type of input
def check_string_type(x, mapping_function: str):
  if isinstance(x, str):
    return(True)
  else:
    raise TypeError(f"Input of `{mapping_function}` needs to be a string (data of type `str`). Not a {type(x)}.")


    
### ====================================================================================
### We use the Mapping class to store the default available mapping methods
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



