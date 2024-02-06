''' Basic classes used to implement different sensors.'''

import abc
from typing import List

import pandas as pd

from general import type_check

class DataExporter(abc.ABC):
    ''' An abstract class to export data to different output, such as csv and database.'''

    def __init__(self) -> None:
        ''' An abstract class to export data to different output, such as csv and database.'''
        pass

    @abc.abstractmethod
    async def export(self, data: pd.DataFrame) -> None:
        ''' Export data to specific output.'''
        return NotImplemented
    

class CsvExporter(DataExporter, abc.ABC):
    ''' An abstract class to export data to csv.'''

    def __init__(self) -> None:
        ''' An abstract class to export data to csv.'''
        super().__init__()
    
    @abc.abstractmethod
    def _generate_path(self) -> str:
        '''
        Generate the path to export data. \ 
        Each sensor has its storage requirements. This method should be implemented based on those requirements.
        '''
        return NotImplemented
    

class DatabaseExporter(DataExporter, abc.ABC):
    ''' An abstract class to export data to a database. '''

    def __init__(self) -> None:
        ''' An abstract class to export data to a database. '''
        super().__init__()

    @abc.abstractmethod
    async def connect(self, config: dict) -> None:
        '''
        Connect to the database. \ 
        Should be implemented based on different database systems and tables.
        '''
        return NotImplemented
    

class DataGenerator():
    ''' 
    A class to subscribe exporters and export data. 
    Please call `notify_exporters` to export data.
    '''

    def __init__(self) -> None:
        ''' 
        A class to subscribe exporters and export data. 
        Please call `notify_exporters` to export data.
        '''
        self.__exporters: List[DataExporter] = []

    def __str__(self) -> str:

        descriptions = ["DataGenerator. Exporters: "]
        descriptions = descriptions + self.list_exporters()
        # To restrict the length of the descriptions, 
        # str(DataGenerator) should not contain new line.
        return "\n".join(descriptions) 

    def add_exporter(self, exporter: DataExporter) -> None:
        '''
        Add a `DataExporter` to the exporter list. \ 
        * param exporter: an implemented `DataExporter` object.
        '''

        type_check(exporter, "exporter", DataExporter)
        self.__exporters.append(exporter)

    async def notify_exporters(self, data: pd.DataFrame) -> None:
        '''
        Export the data throuhg all `DataExporter` objects in the exporter list.  \ 
        If the list is empty, nothing happens.
        * param data: a `DataFrame` object.
        '''

        type_check(data, "data", pd.DataFrame)
        for exporter in self.__exporters:
            await exporter.export(data)

    def list_exporters(self) -> List[str]:
        ''' Return the description of exporters in the exporter list.'''

        descriptions = []
        for exporter in self.__exporters:
            descriptions.append(str(exporter))
        return descriptions


class Reader(DataGenerator, abc.ABC):
    '''
    An abstract reader class. \ 
    A Reader object is used to read data from a type of sensor.
    '''

    def __init__(self, length: int) -> None:
        '''
        An abstract reader class. \ 
        A Reader object is used to read data from a type of sensor.
        '''

        type_check(length, "length", int)
        self._LENGTH_OF_A_BATCH = length
        super().__init__()

    @abc.abstractmethod
    async def read(self) -> pd.DataFrame:
        '''
        Read data from the sensor.\ 
        Any operation, such as connect and close, should be done in this method to unify usages.
        '''
        return NotImplemented


class ModbusReader(Reader, abc.ABC):
    '''
    An abstract class for reading from modbus rtu, tcp, etc.
    A Reader object is used to read data from a type of sensor.
    '''

    def __init__(self, length: int, duration: float, slave: int) -> None:
        '''
        An abstract class for reading from modbus rtu, tcp, etc.
        A Reader object is used to read data from a type of sensor.
        * param length: number of records read in a call of `read()`.
        * param duration: the duration between two reading in each call of `read().
        * param slave: port number in modbus.
        '''

        type_check(duration, "duration", float)
        type_check(slave, "slave", int)

        self._DURATION = duration
        self._SLAVE = slave
        super().__init__(length)
    

class Filter(DataGenerator, abc.ABC):
    ''' A class to process `DataFrame`. It should be used with a `Pipeline` object. '''

    def __init__(self) -> None:
        ''' A class to process `DataFrame`. It should be used with a `Pipeline` object. '''
        super().__init__()

    @abc.abstractmethod
    async def process(self, data: pd.DataFrame) -> pd.DataFrame:
        '''
        Process and return the data.
        Remember to call `self.notify_exporters` if needed.
        '''
        return NotImplemented
    

class Pipeline:
    ''' A pipeline of `Filter`s. '''

    def __init__(self) -> None:
        ''' A pipeline of `Filter`s. '''
        self.__filters: List[Filter] = []

    def __str__(self) -> str:
        
        descriptions = ["Pipeline. Filters: "] + self.list_filters()
        return "\n".join(descriptions)

    def add_filter(self, filter: Filter):
        ''' Add a `Filter` object to the filter list. '''

        type_check(filter, "filter", Filter)
        self.__filters.append(filter)

    async def run(self, data: pd.DataFrame) -> pd.DataFrame:
        ''' Call every filter in the filter list and notify exporters. '''

        type_check(data, "data", pd.DataFrame)
        for filter in self.__filters:
            data = await filter.process(data)
            await filter.notify_exporters(data)
        return data
    
    def list_filters(self) -> List[str]:
        ''' Return the description of filters in the filter list.'''

        descriptions = []
        for filter in self.__filters:
            descriptions.append(str(filter))
        return descriptions
    

class Sensor(abc.ABC, DataGenerator):
    '''
    An abstract class to define a sensor. \ 
    A `Sensor` should have the ability to connect and read data from a source, 
    process and return data using `Filter` and `PipeLine`.
    '''

    def __init__(
            self, 
            reader: Reader, 
            pipeline: Pipeline,
            name: str,
            waiting_time: int
    ) -> None:
        '''
        An abstract class to define a sensor. \ 
        A `Sensor` should have the ability to connect and read data from a source, 
        process and return data using `Filter` and `PipeLine`.

        * param reader: a `Reader` object to read data from the sensor.
        * param pipeline: a `Pipeline` object to process read data.
        * param name: the given name of this sensor for identification.
        * param waiting_time: the waiting time between two reader.read() calls.
        '''

        type_check(reader, "reader", Reader)
        type_check(pipeline, "pipeline", Pipeline)
        type_check(name, "name", str)
        type_check(waiting_time, "waiting_time", int)

        self.READER: Reader = reader
        self.PIPELINE: Pipeline = pipeline
        self.NAME: str = name
        self.WAITING_TIME: int = waiting_time
        super().__init__()

    @abc.abstractmethod
    async def is_alive(self) -> bool:
        ''' Is this sensor connected. Used by `SensorManager`. '''
        return NotImplemented
    
    async def run(self) -> pd.DataFrame:
        ''' Read and return processed data. '''

        data = await self.READER.read()
        await self.READER.notify_exporters(data)
        return await self.PIPELINE.run(data)