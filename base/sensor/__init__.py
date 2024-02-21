import asyncio
from abc import ABC, abstractmethod

from pandas import DataFrame

from base.pipeline import Pipeline
from base.export import DataGenerator
from base.manage import Worker
from general import type_check


class Sensor(DataGenerator, ABC):
    """An abstract sensor class which can read data from any source 
    and register pipelines. 
    """

    def __init__(self, length: int) -> None:
        """An abstract sensor class which can read data from any source 
        and register pipelines.
        
        :param length: the number of data read in one call of `read()` (a batch).        
        """

        type_check(length, "length", int)
        self._LENGTH_OF_A_BATCH = length
        self.__pipelines: list[Pipeline] = []
        super().__init__()

    @abstractmethod
    async def read_and_process(self) -> DataFrame:
        """Read a batch of data from some kind of source and then process it in pipelines. 
        Read from source such as Modbus, WebSocket, CSV.
        """
        return NotImplemented

    def add_pipeline(self, pipeline: Pipeline) -> None:
        """Register a pipeline to process read data."""
        type_check(pipeline, "pipeline", Pipeline)
        self.__pipelines.append(pipeline)

    async def notify_pipelines(self, data: DataFrame) -> None:
        """Notify pipeline in the list to process read data. 

        Return None. Add `DataExporter` to pipelines to receive processed data.
        """

        type_check(data, "data", DataFrame)

        tasks = []
        for pipeline in self.__pipelines:
            tasks.append(asyncio.create_task(pipeline.run(data.copy(deep=True))))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                self.notify_manager(sign=self, content=result)

    @abstractmethod
    async def is_alive(self) -> bool:
        return NotImplemented