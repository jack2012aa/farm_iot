"""Define some common filters."""

from pandas import DataFrame

from base.pipeline import Filter
from general import type_check


class StdFilter(Filter):
    """Compute the standard deviation of data and replace out-of-range data to average +- one std."""

    def __init__(self) -> None:
        """Compute the standard deviation of data and replace out-of-range data to average +- one std."""
        super().__init__()

    def __str__(self) -> str:
        return "StdFilter. Range: [-avg + std, +avg + std]"

    async def process(self, data: DataFrame) -> DataFrame:
        """Compute the standard deviation of data and replace out-of-range data to average +- one std."""

        type_check(data, "data", DataFrame)
        standard_deviation = data.std()
        average = data.mean()

        for i in range(1, data.shape[1]):
            # Define a function here to get std, avg and i.
            def threshold(value):
                value = min(value, average.iloc[i] + standard_deviation.iloc[i])
                value = max(value, average.iloc[i] - standard_deviation.iloc[i])
                return value
            data.iloc[:, i] = data.iloc[:, i].map(threshold)

        return data


class BatchAverageFilter(Filter):
    """ Compute and return the average of weights. `datetime` will be set as last timestamp in original data. """

    def __init__(self) -> None:
        """ Compute and return the average of weights. `datetime` will be set as last timestamp in original data. """
        super().__init__()

    def __str__(self) -> str:
        return "BatchAverageFilter. "

    async def process(self, data: DataFrame) -> DataFrame:
        """ Compute and return the average of weights. `Datetime` will be set as last timestamp in original data. """

        type_check(data, "data", DataFrame)
        average = data.mean().get("weight")
        return DataFrame(data={"datetime":[data.iloc[data.shape[0] - 1, 0]], "weight": [average]})