"""Define filters used for time series. The first column of the dataframe 
have to be Timestamp.
"""

__all__ = [
    "BatchStdevFilter", 
    "BatchAverageFilter", 
    "TimeFilter", 
    "FIFOFilter", 
    "MovingAverageFilter", 
    "MovingMedianFilter", 
    "MovingStdevFilter", 
    "AccumulateFilter"
]

import logging
from abc import ABC
from datetime import datetime
from collections import deque
from statistics import median, stdev, mean

from pandas import DataFrame, Series, isna, concat

from general import type_check
from base.manage import Manager
from base.pipeline import Filter, Pipeline
from base.export.common_exporters import ExporterFactory


class BatchStdevFilter(Filter):
    """Compute the standard deviation of data and replace out-of-range data to 
    average +- n * std.
    """

    def __init__(self, n: float = 1, remove: bool = False) -> None:
        """Compute the standard deviation of data and replace out-of-range data 
        to average +- n * std.
        
        If the length of data is less than 3, it won't be processed since 
        calculating standard deviation needs at least 3 data.

        ## Example
        * n = 1, average = 10, stdev = 2, remove = False. If 8 <= value <= 12, 
        it will be kept same; else, it will be set to 8 or 12.
        * n = 2, average = 10, stdev = 2, remove = True. If 6 <= value <= 14, 
        it will be kept same; else, it will be set to Nan.
        
        :param n: the range of standard deviation.
        :param remove: set true to remove out of range value from the dataframe.
        """
        super().__init__()
        n = float(n)
        type_check(remove, "remove", bool)
        self.__N = n
        self.__REMOVE = remove

    def __str__(self) -> str:
        return f"BatchStdFilter (n={self.__N}, remove={self.__REMOVE})."

    async def process(self, data: DataFrame) -> DataFrame:

        type_check(data, "data", DataFrame)

        raw_data = data.copy()
        for i in range(data.shape[1]):
            if i == 0:
                continue
            values = data.iloc[:, i].dropna().to_list()
            if len(values) <= 2:
                logging.warning("Data is too short to calculate standard deviation.")
                return raw_data
            standard_deviation = stdev(values)
            average = mean(values)

            for j in range(data.shape[0]):
                lower_bound = average - self.__N * standard_deviation
                upper_bound = average + self.__N * standard_deviation
                #If value in range.
                if  lower_bound <= data.iloc[j, i] <= upper_bound:
                    continue
                if self.__REMOVE:
                    data.iloc[j, i] = None
                elif data.iloc[j, i] < lower_bound:
                    data.iloc[j, i] = lower_bound
                else:
                    data.iloc[j, i] = upper_bound

        return data


class BatchAverageFilter(Filter):
    """Compute and return the average of a batch. `Datetime` will be set 
    as the last timestamp in original data. """

    def __init__(self) -> None:
        """Compute and return the average of a batch. `Datetime` will be 
        set as the last timestamp in original data. """
        super().__init__()

    def __str__(self) -> str:
        return "BatchAverageFilter."

    async def process(self, data: DataFrame) -> DataFrame:
        """Compute and return the average of a batch. `Datetime` will be set 
        as the last timestamp in original data. """

        type_check(data, "data", DataFrame)
        output_data = {}
        for i in range(data.shape[1]):
            key = data.keys()[i]
            output_data[key] = []
            if i == 0:
                output_data[key].append(data.iloc[-1, i])
                continue
            values = data.get(key).dropna().to_list()
            if len(values) == 0:
                output_data[key] = [None]
                continue
            output_data[key] = [mean(values)]
        
        return DataFrame(output_data)
    

class TimeFilter(Filter):
    """Transform seperate time info in the DataFrame into datetime."""

    def __init__(self, data_name: list[str], new_data_name: list[str] = None) -> None:
        """Transform seperate time info in the DataFrame into datetime.
        
        :param data_name: list of data name that is not related to time in the df.
        :param new_data_name: if you want to change the name of data in data_name.
        """
        self.data_name = data_name
        self.new_data_name = new_data_name
        super().__init__()

    async def process(self, data: DataFrame) -> DataFrame:

        new_data = {"Timestamp":[]}
        if self.new_data_name is not None:
            for name in self.new_data_name:
                new_data[name] = []
        else:
            for name in self.data_name:
                new_data[name] = []

        for _, row in data.iterrows():
            dt = datetime(
                year=int(float(str(row.get("year")))),
                month=int(float(str(row.get("month")))),
                day=int(float(str(row.get("day")))),
                hour=int(float(str(row.get("hour")))),
                minute=int(float(str(row.get("minute")))),
                second=int(float(str(row.get("second")))),
                microsecond=int(float(str(row.get("millisecond"))))
            )
            new_data["Timestamp"].append(dt)
            if self.new_data_name is not None:
                for old_name, new_name in list(zip(self.data_name, self.new_data_name)):
                    new_data[new_name].append(row.get(old_name))
            else:
                for name in self.data_name:
                    new_data[name].append(row.get(name))

        df = DataFrame(new_data)
        await self.notify_exporters(df)
        return df
    
    
class FIFOFilter(Filter, ABC):
    """An abstract class that keeps a dataframe and maintains it in a fix 
    length based on first in first out. It can be used to implement filters 
    related to moving average or anything similar.
    
    Remember to initialize the dataframe attribute when the first dataframe 
    is read.
    """
    
    def __init__(self, length: int) -> None:
        """An abstract class that keeps a dataframe and maintains it in a fix 
        length based on first in first out. It can be used to implement filters 
        related to moving average or anything similar.

        Remember to initialize the dataframe attribute when the first dataframe 
        is read.
        
        :param length: the maximum number of rows of the kept dataframe.
        :raises: TypeError.
        """
        super().__init__()
        try:
            type_check(length, "length", int)
        except TypeError:
            print("Fail to initialize FIFOFilter because parameter \"length\" is not integer.")
            logging.error("Fail to initialize FIFOFilter because parameter \"length\" is not integer.")
            raise
        self._MAX_LENGTH = length
        self._data = None
        
    def initialize_data(self, dataframe: DataFrame) -> None:
        """Initialize stored data. If the dataframe is larger than MAX_LENGTH 
        some data will lost.
        
        :param dataframe: DataFrame to be stored.
        """

        type_check(dataframe, "dataframe", DataFrame)
        #Deque is faster.
        self._data = {}
        for key in dataframe.keys():
            self._data[key] = deque([], maxlen=self._MAX_LENGTH)
        
    def _replace_first_n(self, data: DataFrame) -> None:
        """Replace the first n rows in the kept dataframe with new data using 
        FIFO.
        
        :param data: new rows to be inserted.
        :raises: TypeError, KeyError.
        """
        
        type_check(data, "data", DataFrame)
        for key in data.keys():
            #Using df.to_dict() will convert datetime to timestamp.
            #So convert to Series first.
            series = data.get(key)
            new_rows = series.to_list()
            for value in new_rows:
                self._data[key].append(value)
                
    def _generate_dataframe(self) -> DataFrame | None:
        """Generate a dataframe using stored data. Return None if no data 
        exists.
        """
        
        if self._data is None:
            return None
        
        dict_series = {}
        for key, queue in self._data.items():
            dict_series[key] = Series(queue)
        return DataFrame(dict_series)
    
    
class MovingAverageFilter(FIFOFilter):
    """Convert data i to a moving average of former n records (with i)."""
    
    def __init__(self, length: int) -> None:
        """Convert data i to a moving average of former n records (with i).

        If meeting a nan value in the data: 
        1. Result will be nan if there is no previous data.
        2. Result will be the average of previous data. This nan is ignored.
        
        :param length: The number of records to calculate the moving average.
        """
        super().__init__(length)
        
    async def process(self, data: DataFrame) -> DataFrame:
        
        if self._data is None:
            self.initialize_data(data)
        
        result = {}
        for i in range(data.shape[1]):
            key = data.keys()[i]
            if i == 0:
                time = data.get(key).to_list()
                result[key] = time
                continue
            moving_averages = []
            values = data.get(key).to_list()
            for value in values:
                if isna(value):
                    if len(self._data[key]) == 0:
                        moving_averages.append(None)
                    else:
                        moving_averages.append(mean(self._data[key]))
                    continue
                self._data[key].append(value)
                moving_averages.append(mean(self._data[key]))
            result[key] = moving_averages

        return DataFrame(result)
    
    
class MovingMedianFilter(FIFOFilter):
    """Convert data i to a median of former n records (with i)."""
    
    def __init__(self, length: int) -> None:
        """Convert data i to a median of former n records (with i).
        
        :param length: The number of records to calculate the median.
        """
        
        super().__init__(length)
        
    async def process(self, data: DataFrame) -> DataFrame:
        
        if self._data is None:
            self.initialize_data(data)

        result = {}
        for i in range(data.shape[1]):
            key = data.keys()[i]
            if i == 0:
                time = data.get(key).to_list()
                result[key] = time
                continue
            moving_medians = []
            values = data.get(key).to_list()
            for value in values:
                if isna(value):
                    if len(self._data[key]) == 0:
                        moving_medians.append(None)
                    else:
                        moving_medians.append(median(self._data[key]))
                    continue
                self._data[key].append(value)
                moving_medians.append(median(self._data[key]))
            result[key] = moving_medians

        return DataFrame(result)


class MovingStdevFilter(FIFOFilter):
    """Remove records out of n standard deviation in a range of records.
    
    For example, if avg = 10, std = 2, n = 2, then data > 14 and data < 6 will 
    be removed from the batch.
    Standard deviation and average are calculated using records in a range.

    If the value is nan, result is nan. 

    If previous data is not enough to calculate standard deviation, the 
    value is kept the same. 
    """
    
    def __init__(self, n: float, length: int, remove: bool = False) -> None:
        """Remove records out of n standard deviation in a batch.
    
        For example, if avg = 10, std = 2, n = 2, then data > 14 and data < 6 will 
        be removed from the batch.
        Standard deviation and average are calculated using the batch data.

        If the value is nan, result is nan.

        If previous data is not enough to calculate standard deviation, the 
        value is kept the same. 
        
        :param n: the number of standard deviation of range.
        :param length: the number of records included to calculate stdev.
        :param remove: set true to remove out of range data.
        """
        super().__init__(length)
        self.__N = float(n)
        type_check(remove, "remove", bool)
        self.__REMOVE = remove
        
    async def process(self, data: DataFrame) -> DataFrame:
        
        type_check(data, "data", DataFrame)
        if self._data is None:
            self.initialize_data(data)

        for i in range(1, data.shape[1]):
            key = data.keys()[i]
            for j in range(data.shape[0]):
                value = data.iloc[j, i]
                if isna(value):
                    continue
                self._data[key].append(value)
                if len(self._data[key]) <= 2:
                    continue
                standard_deviation = stdev(self._data[key])
                average = mean(self._data[key])
                upper_bound = average + self.__N * standard_deviation
                lower_bound = average - self.__N * standard_deviation
                if lower_bound <= value <= upper_bound:
                    continue
                if self.__REMOVE:
                    data.iloc[j, i] = None
                    continue
                
                if data.iloc[j, i] < lower_bound:
                    data.iloc[j, i] = lower_bound
                else:
                    data.iloc[j, i] = upper_bound

        return data


class AccumulateFilter(Filter):
    """Accumulate records until the dataframe has at most n rows.
    
    This filter is used with batch filters. For example, if you have a batch 
    average filter and then an accumulate filter, then the accumulate filter 
    can generate a new 'batch' of data. 

    ## Example:
    If filter.process(data) is called and 
    1. Rows of data + rows of historical data in this filter object < n, then 
    the rows of returned data will be smaller than n.
    2. Rows of data + rows of historical data > n, then the rows of returned 
    data will be exactly n (historical data and front data). Data that doesn't 
    contain in this batch will be saved.
    """

    def __init__(self, length: int) -> None:
        """Accumulate records until the dataframe has at most n rows.
    
        This filter is used with batch filters. For example, if you have a batch 
        average filter and then an accumulate filter, then the accumulate filter 
        can generate a new 'batch' of data. 

        ## Example:
        If filter.process(data) is called and 
        1. Rows of data + rows of historical data in this filter object < n, then 
        the rows of returned data will be smaller than n.
        2. Rows of data + rows of historical data > n and rows of data < n, then 
        the rows of returned data will be exactly n (historical data and front data). 
        Data that doesn't contain in this batch will be saved.
        3. Rows of data + rows of historical data > 2n, then only the last n 
        rows in the new data are returned. Historial data is popped.

        :param length: the maximum number of rows to be accumulated.
        """
        super().__init__()
        self.__MAX_LENGTH = length
        self.__old = DataFrame()

    async def process(self, data: DataFrame) -> DataFrame:
        
        type_check(data, "data", DataFrame)

        if data.shape[0] + self.__old.shape[0] >= 2 * self.__MAX_LENGTH:
            self.__old = DataFrame()
            return DataFrame(data.iloc[-self.__MAX_LENGTH:, :])
        elif data.shape[0] + self.__old.shape[0] < self.__MAX_LENGTH:
            self.__old = concat([self.__old, data], ignore_index=True)
            return self.__old.copy()
        else:
            export_rows = self.__MAX_LENGTH - self.__old.shape[0]
            old_copy = self.__old.copy()
            self.__old = DataFrame(data.iloc[export_rows + 1:, :])
            return concat([old_copy, data.iloc[:export_rows, :]], ignore_index=True)


class PipelineFactory():
    """A factory class to build pipeline using dictionary."""

    def __init__(self) -> None:
        pass

    def create(self, settings: list, manager: Manager = None) -> Pipeline:
        """Create a pipeline using the settings. 

        Settings should look like this:

        [
            {
                #Type and filter specific settings.
                "type": StdFilter, 
                "exporters":[exp1: dict, exp2: dict, ...]
            }
        ]

        Please refer to example json for detail.

        :param settings: settings of the pipeline, including filters, exporters, etc.
        :param manager: exporters' manager.
        :raise: ValueError, KeyError.
        """

        type_check(settings, "settings", list)

        pipeline = Pipeline()
        for filter_setting in settings:

            filter = None
            #Remember to add new filter type to here in the future.
            try:
                match filter_setting["type"]:
                    case "StdFilter":
                        filter = BatchStdevFilter()
                    case "BatchAverageFilter":
                        filter = BatchAverageFilter()
                    case "MovingAverageFilter":
                        filter = MovingAverageFilter(filter_setting["max_length"])
                    case _:
                        print(f"Filter type {filter_setting["type"]} does not exist.")
                        raise ValueError
            #Required specific setting not found.
            except KeyError as ex:
                print(f"Filter type {filter_setting["type"]} misses setting \"{ex.args[0]}\"")

            #Construct exporters.
            exporter_factory = ExporterFactory()
            exporters_settings = filter_setting.get("exporters")
            #Avoid nesting.
            if exporters_settings is None:
                exporters_settings = []
            for exporter in exporters_settings:
                new_exporter = exporter_factory.create(exporter)
                new_exporter.set_manager(manager)
                filter.add_exporter(new_exporter)
            pipeline.add_filter(filter)

        return pipeline
