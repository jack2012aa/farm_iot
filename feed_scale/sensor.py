from pandas.core.api import DataFrame as DataFrame

from basic_sensor import Pipeline, Reader, Sensor, ModbusReader


class FeedScale(Sensor):

    def __init__(self, reader: Reader, pipeline: Pipeline, name: str, waiting_time: int = 0) -> None:
        super().__init__(reader, pipeline, name, waiting_time)
        self.__alive: bool = False

    async def is_alive(self) -> bool:
        return self.__alive
    
    async def run(self) -> DataFrame:
        
        data = await super().run()
        if data.size > 0:
            self.__alive = True
        else:
            self.__alive = False
        return data