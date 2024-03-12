"""Define some common filters."""

import asyncio
import logging
from abc import ABC
from datetime import datetime
from collections import deque
from statistics import median, stdev

from tqdm import tqdm
from numpy import nan
from pandas import DataFrame, Series

from general import type_check
from base.manage import Manager
from base.pipeline import Filter, Pipeline
from base.export.common_exporters import ExporterFactory


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
        average = data.mean()
        average.iloc[0] = data.iloc[data.shape[0] - 1, 0]
        df = average.to_frame().T
        # Change dtype back.
        df[df.columns[0]] = df[df.columns[0]].astype(data[data.columns[0]].dtype)
        
        return df
    

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
        
        :param length: The number of records to calculate the moving average.
        """
        super().__init__(length)
        
    async def process(self, data: DataFrame) -> DataFrame:
        
        if self._data is None:
            self.initialize_data(data)
        
        result = {}
        #Set progress bar.
        if data.shape[0] > 3000:
            progress_bar = tqdm(total=data.shape[0] * (data.shape[1] - 1), desc="Moving Average Filter")
        else:
            progress_bar = None
            
        #Assume that column 0 is timestamp. Skip column 0.
        for i in range(data.shape[1]):
            key = data.keys()[i]
            if i == 0:
                time = data.get(key).to_list()
                result[key] = time
                continue
            moving_averages = []
            values = data.get(key)
            for value in values:
                self._data[key].append(value)
                moving_averages.append(sum(self._data[key]) / len(self._data[key]))
                if progress_bar is not None:
                    progress_bar.update(1)
            result[key] = moving_averages
        if progress_bar is not None:
            progress_bar.close()

        await asyncio.sleep(0)
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
        
        if data.shape[0] > 3000:
            progress_bar = tqdm(total=data.shape[0] * (data.shape[1] - 1), desc="Moving Median Filter")
        else:
            progress_bar = None
        
        #Assume that column 0 is timestamp. Skip column 0.
        for i in range(data.shape[1]):
            key = data.keys()[i]
            if i == 0:
                time = data.get(key).to_list()
                result[key] = time
                continue
            medians = []
            values = data.get(key)
            for value in values:
                self._data[key].append(value)
                medians.append(median(self._data[key]))
            result[key] = medians
            if progress_bar is not None:
                progress_bar.update(1)
        if progress_bar is not None:
            progress_bar.close()
            
        await asyncio.sleep(0)
        return DataFrame(result)


class BatchStdevRangeFilter(Filter):
    """Remove records out of n standard deviation in a batch.
    
    For example, if avg = 10, std = 2, n = 2, then data > 14 and data < 6 will 
    be removed from the batch.
    Standard deviation and average are calculated using the batch data.
    """
    
    def __init__(self, n: int | float) -> None:
        """Remove records out of n standard deviation in a batch.
    
        For example, if avg = 10, std = 2, n = 2, then data > 14 and data < 6 will 
        be removed from the batch.
        Standard deviation and average are calculated using the batch data.
        
        :param n: the number of standard deviation of range.
        """
        super().__init__()
        self.__N = n
        
    async def process(self, data: DataFrame) -> DataFrame:
        
        type_check(data, "data", DataFrame)
        
        if data.shape[0] > 3000:
            progress_bar = tqdm(total=data.size - data.shape[0], desc="Batch stdev Range Filter")
        else:
            progress_bar = None
            
        for i in range(data.shape[1]):
            key = data.keys()[i]
            if i == 0:
                continue
            records = data.get(key).to_list()
            average = sum(records) / len(records)
            std = stdev(records)
            upper_bound = average + self.__N * std
            lower_bound = average - self.__N * std
            for j in range(len(records)):
                #Get each record from dataframe.
                if data.iloc[j, i] > upper_bound or data.iloc[j, i] < lower_bound:
                    data.iloc[j, i] = nan
            if progress_bar is not None:
                progress_bar.update(1)
        if progress_bar is not None:
            progress_bar.close()
                    
        #Remove rows that are all Nan in each column except timestamp.
        data.dropna(thresh=data.shape[1] - 1, inplace=True)
        await asyncio.sleep(0)
        return data


class MovingStdevRangeFilter(FIFOFilter):
    """Remove records out of n standard deviation in a range of records.
    
    For example, if avg = 10, std = 2, n = 2, then data > 14 and data < 6 will 
    be removed from the batch.
    Standard deviation and average are calculated using records in a range.
    """
    
    def __init__(self, n: int | float, length: int) -> None:
        """Remove records out of n standard deviation in a batch.
    
        For example, if avg = 10, std = 2, n = 2, then data > 14 and data < 6 will 
        be removed from the batch.
        Standard deviation and average are calculated using the batch data.
        
        :param n: the number of standard deviation of range.
        :param length: the number of records included to calculate stdev.
        """
        super().__init__(length)
        self.__N = n
        
    async def process(self, data: DataFrame) -> DataFrame:
        
        type_check(data, "data", DataFrame)
        if self._data is None:
            self.initialize_data(data)
        if data.shape[0] > 3000:
            progress_bar = tqdm(total=data.size - data.shape[0], desc="Moving stdev Range Filter")
        else:
            progress_bar = None
        
        for i in range(data.shape[1]):
            key = data.keys()[i]
            if i == 0:
                continue
            records = data.get(key).to_list()
            for j, record in zip(range(len(records)), records):
                self._data[key].append(record)
                #Cannot compute stdev when n < 2.
                if len(self._data[key]) < 2:
                    continue
                average = sum(self._data[key]) / len(self._data[key])
                std = stdev(self._data[key])
                upper_bound = average + self.__N * std
                lower_bound = average - self.__N * std
                #Get each record from dataframe.
                if data.iloc[j, i] > upper_bound or data.iloc[j, i] < lower_bound:
                    data.iloc[j, i] = nan
                if progress_bar is not None:
                    progress_bar.update(1)
        if progress_bar is not None:
            progress_bar.close()
                    
        #Remove rows that are all Nan in each column except timestamp.
        data.dropna(thresh=data.shape[1] - 1, inplace=True)
        await asyncio.sleep(0)
        return data


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
                        filter = StdFilter()
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
