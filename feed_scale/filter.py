from pandas import DataFrame

from basic_sensor import Filter
from general import type_check


class StdFilter(Filter):
    ''' Compute the standard deviation of data and replace out-of-range data to average +- one std.'''

    def __init__(self) -> None:
        super().__init__()

    async def process(self, data: DataFrame) -> DataFrame:
        ''' Compute the standard deviation of data and replace out-of-range data to average +- one std.'''
        
        type_check(data, "data", DataFrame)
        standard_deviation = round(data.std().get("weight"), 2)
        average = round(data.mean().get("weight"), 2)

        for i in range(data.shape[0]):
            data.iloc[i, 1] = min(data.iloc[i, 1], average + standard_deviation)
            data.iloc[i, 1] = max(data.iloc[i, 1], average - standard_deviation)

        return data
    

class BatchAverageFilter(Filter):
    ''' Compute and return the average of weights. `datetime` will be set as last timestamp in original data. '''

    def __init__(self) -> None:
        super().__init__()

    async def process(self, data: DataFrame) -> DataFrame:
        ''' Compute and return the average of weights. `Datetime` will be set as last timestamp in original data. '''
        
        type_check(data, "data", DataFrame)
        average = data.mean().get("weight")
        return DataFrame(data={"datetime":[data.iloc[data.shape[0] - 1, 0]], "weight": [average]})