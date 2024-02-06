from pandas.core.api import DataFrame as DataFrame

from basic_sensor import Pipeline, Reader, Sensor


class FeedScale(Sensor):
    ''' A basic `Sensor` class to represent a feed scale device.'''

    def __init__(self, reader: Reader, pipeline: Pipeline, name: str, waiting_time: int = 0) -> None:
        super().__init__(reader, pipeline, name, waiting_time)
        self.__alive: bool = False

    def __str__(self) -> str:
        descriptions = [
            "FeedScale", 
            "----------------------------------------------------------", 
            f"Name: {self.NAME}",
            f"waiting time: {str(self.WAITING_TIME)}",
            "----------------------------------------------------------", 
            f"Reader: {str(self.READER)}", 
            "----------------------------------------------------------", 
            f"Pipeline: {str(self.PIPELINE)}"
        ]
        return "\n".join(descriptions)

    async def is_alive(self) -> bool:
        return self.__alive
    
    async def run(self) -> DataFrame:
        
        data = await super().run()
        if data.size > 0:
            self.__alive = True
        else:
            self.__alive = False
        return data