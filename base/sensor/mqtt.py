import asyncio

from pandas import DataFrame

from base.gateway.mqtt import MQTTClientManager
from base.sensor import Sensor
from general import type_check

class MQTTBasedSensor(Sensor):
    """ An abstract class obtains MQTT messages from MQTTClientManager using 
    asyncio.Queue.
    
    :param length: the number of data read in one call of `read()` (a batch).
    :param name: name of the `Sensor`.
    :param data_topic: the topic that sensor publish to the broker.
    :param heartbeat_topic: the topic that sensor publish heartbeat message.
    :param belonging: the belonging of this sensor, who are in charge of it.
    """
    
    def __init__(
        self, 
        length: int, 
        name: str, 
        data_topic: str,
        heartbeat_topic: str,
        belonging: tuple[str] = tuple()
    ) -> None:
        
        type_check(data_topic, "data_topic", str)
        type_check(heartbeat_topic, 'heartbeat_topic', str)
        mqtt_client = MQTTClientManager()
        # Can't await in __init__. If subscription failed due to internet 
        # error, error may happen somewhere else.
        asyncio.create_task(mqtt_client.subscribe(data_topic))
        asyncio.create_task(mqtt_client.subscribe(heartbeat_topic))
        self.__async_data_queue = mqtt_client.get_topic_queue(data_topic)
        self.__heartbeat_queue = mqtt_client.get_topic_queue(heartbeat_topic)
        super().__init__(length, name, 0, belonging)
        
    async def read_and_process(self) -> DataFrame:
        
    async def is_alive(self) -> bool: