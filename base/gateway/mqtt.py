__all__ = [
    "MQTTClientManager"
]

import os
import json
import asyncio
import logging

import paho.mqtt.client as mqtt

from base.manage import Report
from general import type_check
from base.gateway import GatewayManager


class MQTTClientManager(GatewayManager):
    """A class listening to MQTT broker and distributing message to 
    corresponding queue.

    
    The json configuration file should look like this:
    {
        "host": "192.168.1.123", 
        "port": "1883"
    }

    :param path: path of configuration file.
    :raises: TypeError, FileNotFoundError.
    """

    __TOPIC_QUEUE: dict[str, asyncio.Queue] = {}


    def __init__(self, path: str) -> None:

        type_check(path, "path", str)
        if not os.path.isfile(path):
            msg = f"{path} is not a file."
            logging.error(msg)
            raise FileNotFoundError(msg)
        
        with open(path) as file:
            settings = json.load(file)

        # Refer to paho-mqtt for instruction.
        self.__client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.__client.connect(settings["host"], int(settings["port"]))
        self.__client.on_connect = self.__on_connect
        self.__client.on_message = self.__on_message
        self.__client.loop_start()
        super().__init__()

    async def initialize(self, path: str = "") -> None:
        """ Set the running event loop into paho client userdata.
        
        :param path: an unused parameter.
        """
        # Put current event loop into userdata to call queue.put() in another 
        # thread. Does not call in __init__ because it has to in a coroutine 
        # to get the current event loop.
        self.__client.user_data_set({
            "event_loop": asyncio.get_event_loop()
        })

    def get_topic_queue(self, topic: str) -> asyncio.Queue[mqtt.MQTTMessage]:
        """Return an asyncio.Queue contains messages from a topic.

        :param topic: a topic of MQTT message.
        :raises: KeyError.
        """

        try:
            MQTTClientManager.__TOPIC_QUEUE[topic]
            return MQTTClientManager.__TOPIC_QUEUE[topic]
        except Exception as ex:
            logging.error(
                f"Topic \"{topic}\" is not defined or MQTTClientManager does not initialize."
            )
            raise ex

    async def handle(self, report: Report) -> None:
        pass

    def disconnect(self) -> None:
        """Disconnect the mqtt client."""
        self.__client.loop_stop()
        self.__client.disconnect()
        
    def subscribe(self, topic: str) -> None:
        """Subscribe a MQTT topic.
        
        :param topic: a MQTT topic.
        :raises: TypeError.
        """
        
        type_check(topic, "topic", str)
        MQTTClientManager.__TOPIC_QUEUE[topic] = asyncio.Queue()
        self.__client.subscribe(topic, 2)

    async def publish(self, topic: str, payload: str) -> None:
        """ Publish a MQTT message.

        :param topic: a MQTT topic.
        :param payload: the message to be sent.
        """
        info = self.__client.publish(topic, payload)
        while not info.is_published():
            await asyncio.sleep(0)
        return
    
    def __on_connect(self, client, userdata, flags, reason_code, properties):
        """ A callback function to subscribe topics when connect/reconnect."""
        topics = list(MQTTClientManager.__TOPIC_QUEUE.keys())
        if len(topics) > 0:
            client.subscribe([(topic, 2) for topic in topics])

    def __on_message(self, client, userdata, message):
        """ A callback function to distribute message into async queues."""
        loop = userdata["event_loop"]
        topic = message.topic
        asyncio.run_coroutine_threadsafe(
            MQTTClientManager.__TOPIC_QUEUE[topic].put(message), 
            loop
        )
        print("Get a message.")