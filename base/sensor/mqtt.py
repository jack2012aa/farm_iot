__all__ = [
    "MQTTBasedSensor"
]

import asyncio
import logging
from datetime import datetime

from pandas import DataFrame

from base.gateway.mqtt import MQTTClientManager
from base.sensor import Sensor
from general import type_check

class MQTTBasedSensor(Sensor):

    def __init__(
        self, 
        length: int, 
        waiting_time: float, 
        name: str, 
        data_topic: str,
        heartbeat_topic: str,
        timeout: float = 60,
        belonging: tuple[str] = None
    ) -> None:
        """ An abstract class obtains MQTT messages from MQTTClientManager using 
        asyncio.Queue.

        Please use call `await initialize()` to initiate an instance.

        If the sensor is disconnected, read_and_process() will suspend forever. 
        Hence please use is_alive() manually and frequently.
        
        :param length: the number of data read in one call of `read()` (a batch).
        :param waiting_time: the waiting time between two read_and_process().
        :param name: name of the `Sensor`.
        :param data_topic: the topic that sensor publish to the broker.
        :param heartbeat_topic: the topic that sensor publish heartbeat message.
        :param timeout: time to check sensor alive in second.
        :param belonging: the belonging of this sensor, who are in charge of it.
        """

        #type_check does not do type casting.
        if isinstance(waiting_time, int):
            waiting_time = float(waiting_time)
        if isinstance(timeout, int):
            timeout = float(timeout)

        type_check(waiting_time, "duration", float)
        type_check(data_topic, "data_topic", str)
        type_check(heartbeat_topic, 'heartbeat_topic', str)
        type_check(timeout, "timeout", float)

        # Subscribe and get topic queue.
        client = MQTTClientManager()
        client.subscribe(data_topic)
        client.subscribe(heartbeat_topic)
        self.__ASYNC_DATA_QUEUE = client.get_topic_queue(data_topic)
        self.__HEARTBEAT_QUEUE = client.get_topic_queue(heartbeat_topic)

        # Set properties.
        self.__TOPIC = data_topic # Use as a key.
        self.__TIMEOUT = timeout
        super().__init__(length, name, waiting_time, belonging)
        
    async def read_and_process(self) -> DataFrame:
        """ Read a batch of data from MQTT using asyncio.Queue, process in 
        pipeline and sleep for 'duration' second.
        """

        data_dict = {"Timestamp": [], self.__TOPIC: []}
        while len(data_dict["Timestamp"]) < self._LENGTH_OF_A_BATCH:
            data = await self.__ASYNC_DATA_QUEUE.get()
            data_dict["Timestamp"].append(datetime.now())
            # Decode bytes.
            data_dict[self.__TOPIC].append(data.payload.decode())
        data_frame = DataFrame(data_dict)
        await asyncio.gather(
            self.notify_exporters(data_frame),
            self.notify_pipelines(data_frame)
        )
        return data_frame
        
    async def is_alive(self) -> bool:
        """Check whether the sensor is connected through heartbeat message."""
        try:
            # Clear historical message.
            while not self.__HEARTBEAT_QUEUE.empty():
                await self.__HEARTBEAT_QUEUE.get()
            logging.info("Begin waiting...")
            await asyncio.wait_for(
                self.__HEARTBEAT_QUEUE.get(), 
                self.__TIMEOUT
            )
            return True
        except TimeoutError:
            return False