''' Basic classes used to implement different sensors.'''

import abc
from typing import List

import pandas as pd

from general import type_check

class DataExporter(abc.ABC):
    ''' An abstract class to export data to different output, such as csv and database.'''

    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    async def export(self, data: pd.DataFrame) -> None:
        return NotImplemented
    

class CsvExporter(DataExporter, abc.ABC):
    ''' An abstract class to export data to csv.'''

    def __init__(self, dir: str, file_name: str) -> None:

        type_check(dir, "dir", str)
        type_check(file_name, "file_name", str)

        self.__DIR = dir
        self.__FILE_NAME = file_name
        super().__init__()

    def get_dir(self) -> str:
        return self.__DIR
    
    def get_file_name(self) -> str:
        return self.__FILE_NAME
    
    @abc.abstractmethod
    def generate_path(self) -> None:
        return NotImplemented
    

class DatabaseExporter(DataExporter, abc.ABC):
    ''' An abstract class to export data to a database. '''

    def __init__(self) -> None:
        
        super().__init__()

    @abc.abstractmethod
    async def connect(self, config: dict) -> None:
        return NotImplemented
    

class DataGenerator():
    ''' 
    An class to subscribe exporters and export data. 
    Please call `notify_exporters` to export data.
    '''

    def __init__(self) -> None:
        self.__exporters: List[DataExporter] = []

    def add_exporter(self, exporter: DataExporter) -> None:
        type_check(exporter, "exporter", DataExporter)
        self.__exporters.append(exporter)

    async def notify_exporters(self, data: pd.DataFrame) -> None:
        for exporter in self.__exporters:
            await exporter.export(data)


class Reader(DataGenerator, abc.ABC):
    '''
    An abstract reader class. 
    A Reader object is used to read data from a type of sensor.
    '''

    def __init__(self, length: int) -> None:

        type_check(length, "length", int)
        self._LENGTH_OF_A_BATCH = length
        super().__init__()

    @abc.abstractmethod
    async def read(self) -> pd.DataFrame:
        ''' Read data from the sensor.'''
        return NotImplemented


class ModbusReader(Reader, abc.ABC):
    '''
    An abstract class for reading from modbus rtu, tcp, etc.
    A Reader object is used to read data from a type of sensor.
    '''

    def __init__(self, length: int, duration: int) -> None:

        type_check(duration, "duration", int)
        self._DURATION = duration
        super().__init__(length)

    @abc.abstractmethod
    async def connect(self) -> None:
        return NotImplemented
    

class Filter(DataGenerator, abc.ABC):
    '''
    An abstract Filter class.
    A `Filter` class is used to process data frames. It should be used with `Pipeline`
    '''

    def __init__(self) -> None:
        super().__init__()

    @abc.abstractmethod
    async def process(self, data: pd.DataFrame) -> pd.DataFrame:
        '''Remember to call `self.notify_exporters` if needed'''
        return NotImplemented
    

class Pipeline:
    ''' A pipeline of `Filter`s. '''

    def __init__(self) -> None:
        self.__filters: List[Filter] = []

    def add_filter(self, filter: Filter):

        type_check(filter, "filter", Filter)
        self.__filters.append(filter)

    async def process(self, data: pd.DataFrame) -> pd.DataFrame:
        ''' Call every filter in the filter list. '''

        type_check(data, "data", pd.DataFrame)
        for filter in self.__filters:
            data = await filter.process(data)
        return data