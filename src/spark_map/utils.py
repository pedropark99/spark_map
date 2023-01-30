
def __is_string(x):
    return isinstance(x, str)

def __is_list_or_tuple(x):
    return isinstance(x, list) | isinstance(x, tuple)

def __check_string_input(x, mapping_function:str):
    if not __is_string(x):
        raise TypeError(f"Input of `{mapping_function}` needs to be a string (data of type `str`). Not {type(x)}.")
    if x == '':
        raise ValueError(f"Looks like you gave an empty string as input for `{mapping_function}`. This string should not be empty!")




def __check_list_input(x, type, mapping_function:str):
    if not __is_list_or_tuple(x):
        raise TypeError(f"Input of `{mapping_function}` needs to be a list. Not {type(x)}.")
    if len(x) == 0:
        raise ValueError(f"You provided an empty list as input for `{mapping_function}`. However, this list should not be empty!")
    if __is_list_or_tuple(x[0]) and mapping_function == 'at_position()':
        raise ValueError("Did you provided your column indexes inside a list to `at_position()`? You should not encapsulate these indexes inside a list. For example, if you want to select 1° and 3° columns, just do `at_position(1, 3)` instead of `at_position([1, 3])`.")

    are_of_type = map(lambda x: isinstance(x, type), x)
    # Check if all elements are of designated type
    all_elements_of_type = all(are_of_type)
    if not all_elements_of_type:
        raise TypeError(f"`{mapping_function}` should receive a list of `{type}` values. But some of the elements in the input list are not of this type.")