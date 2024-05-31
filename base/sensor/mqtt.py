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
        name: str, 
        data_topic: str,
        heartbeat_topic: str,
        timeout: float = 60,
        belonging: tuple[str] = None
    ) -> None:
        """ An abstract class obtains MQTT messages from MQTTClientManager using 
        asyncio.Queue.

        If the sensor is disconnected, read_and_process() will suspend forever. 
        Hence please use is_alive() manually and frequently.
        
        :param length: the number of data read in one call of `read()` (a batch).
        :param name: name of the `Sensor`.
        :param data_topic: the topic that sensor publish to the broker.
        :param heartbeat_topic: the topic that sensor publish heartbeat message.
        :param timeout: time to check sensor alive in second.
        :param belonging: the belonging of this sensor, who are in charge of it.
        """

        #type_check does not do type casting.
        if isinstance(timeout, int):
            timeout = float(timeout)

        type_check(data_topic, "data_topic", str)
        type_check(heartbeat_topic, 'heartbeat_topic', str)
        type_check(timeout, "timeout", float)

        # Subscribe and get topic queue.
        client = MQTTClientManager()
        client.subscribe(data_topic)
        client.subscribe(heartbeat_topic)
        self._ASYNC_DATA_QUEUE = client.get_topic_queue(data_topic)
        self._HEARTBEAT_QUEUE = client.get_topic_queue(heartbeat_topic)

        # Set properties.
        self._TOPIC = data_topic # Use as a key.
        self._TIMEOUT = timeout
        super().__init__(length, name, 0.0, belonging)
        
    async def read_and_process(self) -> DataFrame:
        """ Read a batch of data from MQTT using asyncio.Queue and process in 
        pipeline.
        """

        data_dict = {"Timestamp": [], self._TOPIC: []}
        while len(data_dict["Timestamp"]) < self._LENGTH_OF_A_BATCH:
            data = await self._ASYNC_DATA_QUEUE.get()
            data_dict["Timestamp"].append(datetime.now())
            # Decode bytes.
            data_dict[self._TOPIC].append(data.payload.decode())
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
            while not self._HEARTBEAT_QUEUE.empty():
                await self._HEARTBEAT_QUEUE.get()
            logging.info("Begin waiting...")
            await asyncio.wait_for(
                self._HEARTBEAT_QUEUE.get(), 
                self._TIMEOUT
            )
            return True
        except TimeoutError:
            return False