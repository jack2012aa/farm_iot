import asyncio
from abc import ABC, abstractmethod

from pandas import DataFrame

from base.pipeline import Pipeline
from base.export import DataGenerator
from base.manage import Manager, Report
from general import type_check


class Sensor(DataGenerator, ABC):
    """An abstract sensor class which can read data from any source 
    and register pipelines. 
    """

    def __init__(
        self, 
        length: int, 
        name: str, 
        waiting_time: float, 
        belonging: tuple[str] = tuple()
    ) -> None:
        """An abstract sensor class which can read data from any source 
        and register pipelines.
        
        :param length: the number of data read in one call of `read()` (a batch).
        :param name: name of the `Sensor`.
        :param waiting_time: waiting time between two batches of reading in second.
        :param belonging: the belonging of this sensor, who are in charge of it.
        """

        type_check(length, "length", int)
        type_check(name, "name", str)
        type_check(waiting_time, "waiting_time", float)
        type_check(belonging, "belonging", tuple)

        self._LENGTH_OF_A_BATCH = length
        self.__pipelines: list[Pipeline] = []
        self.NAME = name
        self.WAITING_TIME = waiting_time
        self.belonging = belonging
        super().__init__()

    async def run(self) -> None:
        """Read and process data continuously in a while loop."""
        while True:
            await self.read_and_process()
            await asyncio.sleep(self.WAITING_TIME)

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
                await self.notify_manager(report=Report(sign=self, content=result))

    @abstractmethod
    async def is_alive(self) -> bool:
        return NotImplemented
    

class SensorManager(Manager, ABC):
    """An abstract class for sensor management."""

    def __init__(self, email_settings: dict = None) -> None:
        """An abstract class for sensor management."""
        super().__init__(email_settings)

    @abstractmethod
    async def run(self) -> None:
        return NotImplemented