''' Some useful functions. '''

def type_check(var, var_name: str, correct_type):
    '''
    * param var: the incorrect variable
    * param var_name: name of the variable.
    * param correct_type: correct type.
    '''

    if not isinstance(var, correct_type):
        raise TypeError(f"{var_name} should be of type {correct_type.__name__}. Got {type(var).__name__} instead.")