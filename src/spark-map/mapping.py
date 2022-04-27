## Helper function to check data type of input
def check_string_type(x, mapping_function: str):
  if isinstance(x, str):
    return(True)
  else:
    raise TypeError(f"Input of `{mapping_function}` needs to be a string (data of type `str`). Not a {type(x)}.")


### Lets use a class to store available mapping methods

class Mapping:
    
    #### PUBLIC MEMBERS (OR DATA) ======================================================
    def __init__(self):
      self.selected_cols = []
    
    
    #### PUBLIC METHODS FOR THE CLASS ==================================================
  
    def all_of(self, list_cols: list):
      return {'fun': '__all_of', 'val': list_cols}

    def matches(self, regex: str):
      check_string_type(regex, "matches()")
      return {'fun': "__matches", 'val': regex} 


    def at_position(self, *indexes, zero_index = False):
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
      return {'fun': "__at_position", 'val': indexes}


    def starts_with(self, text: str):
      check_string_type(text, "starts_with()")
      return {'fun': "__starts_with", 'val': text}

    def ends_with(self, text: str):
      check_string_type(text, "ends_with()")
      return {'fun': "__ends_with", 'val': text}

    def are_of_type(self, arg_type: str):
      check_string_type(arg_type, "are_of_type()")
      valid_types = ['string', 'int', 'double', 'date', 'datetime']
      if arg_type not in valid_types:
        types = [f"'{t}'" for t in valid_types]
        types = ', '.join(types)
        message = f'You must choose one of the following values: {types}'
        raise ValueError(message)
      return {'fun': "__are_of_type", 'val': arg_type}
  
  