""" Some useful functions. """
import random
from datetime import datetime, timedelta

from pandas import isna
from pandas import DataFrame

def type_check(var, var_name: str, correct_type):
    """
    * param var: the incorrect variable
    * param var_name: name of the variable.
    * param correct_type: correct type.
    """

    if not isinstance(var, correct_type):
        raise TypeError(f"{var_name} should be of type {correct_type.__name__}. Got {type(var).__name__} instead.")
    
def generate_time_series(
        lower_bound_of_rows: int = 0, 
        upper_bound_of_rows: int = 100, 
        lower_bound_of_data: int = 1, 
        upper_bound_of_data: int = 114514, 
        number_of_columns: int = 1, 
        generate_none: bool = True, 
        starting_datetime: datetime = datetime.now()
    ) -> DataFrame:
    """Generate random time series dataframe. The number of rows and data 
    value are random. Timestamps are ordered in asc order. Nan may exists 
    in the data.
    
    :param lower_bound_of_rows: the minimum possible number of rows.
    :param upper_bound_of_rows: the maximum possible number of rows.
    :param lower_bound_of_data: the minimum possible value of data.
    :param upper_bound_of_data: the maximum possible value of data.
    :param number_of_columns: the number of attribute excluding timestamp.
    :param generate_none: generate None in 1/20 probability.
    """

    #Initialize dictionary.
    data = {"Timestamp": []}
    for i in range(number_of_columns):
        data[f"values{i}"] = []

    #Generate data
    current_timestamp = starting_datetime
    number_of_rows = random.randint(lower_bound_of_rows, upper_bound_of_rows)
    for _ in range(number_of_rows):
        data["Timestamp"].append(current_timestamp)
        current_timestamp = current_timestamp + timedelta(seconds=random.randint(1, 10))
        for i in range(number_of_columns):
            if generate_none and random.randint(1, 20) == 1:
                data[f"values{i}"].append(None)
                continue
            value = random.normalvariate(
                (upper_bound_of_data + lower_bound_of_data) / 2, 
                (upper_bound_of_data + lower_bound_of_data) / 6, 
            )
            data[f"values{i}"].append(value)

    return DataFrame(data)

def transform_na(value: float | int) -> float | int | None:
    """If the value is pandas.nan, transform it to None."""

    if isna(value):
        return None
    return value