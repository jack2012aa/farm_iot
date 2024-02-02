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

    def __init__(self) -> None:
        super().__init__()
    
    @abc.abstractmethod
    def _generate_path(self) -> str:
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

    def __init__(self, length: int, duration: float, slave: int) -> None:
        '''
        * param length: length of a batch.
        * param duration: the duration between two reading.
        * param slave: port number in modbus.
        '''

        type_check(duration, "duration", float)
        type_check(slave, "slave", int)

        self._DURATION = duration
        self._SLAVE = slave
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
    

class Sensor(abc.ABC, DataGenerator):

    def __init__(
            self, 
            reader: Reader, 
            pipeline: Pipeline,
            name: str,
            waiting_time: int
    ) -> None:
        '''
        * param reader: a Reader object to read data from the sensor.
        * param pipeline: a Pipeline object to process read data.
        * param name: the given name of this sensor for identification.
        * param waiting_time: the waiting time between two reader.read() calls.
        '''

        type_check(reader, "reader", Reader)
        type_check(pipeline, "pipeline", Pipeline)
        type_check(name, "name", str)
        type_check(waiting_time, "waiting_time", int)

        self.__reader: Reader = reader
        self.__pipeline: Pipeline = pipeline
        self.NAME: str = name
        self.WAITING_TIME: int = waiting_time
        super().__init__()

    @abc.abstractmethod
    async def is_alive(self) -> bool:
        return NotImplemented
    
    async def run(self) -> pd.DataFrame:
        ''' Read and return processed data. '''

        data = await self.__reader.read()
        return await self.__pipeline.process(data)