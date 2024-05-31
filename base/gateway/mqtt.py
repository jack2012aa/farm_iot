__all__ = [
    "MQTTClientManager"
]

import os
import json
import asyncio
import logging

import paho.mqtt.client as mqtt

from base.manage import Report
from general import type_check, singleton
from base.gateway import GatewayManager

@singleton
class MQTTClientManager(GatewayManager):
    """A class listening to MQTT broker and distributing message to 
    corresponding queue.

    This is a singleton class. Reinitialize without disconnect() will cause an 
    error.
    """

    def __init__(self) -> None:
        self.__initialized = False
        super().__init__()

    async def initialize(self, path: str = "") -> None:
        """Connect to MQTT broker and start the loop.

        The json configuration file should look like this:
        {
            "host": "192.168.1.123", 
            "port": "1883"
        }

        :param path: path of configuration file.
        :raises: RuntimeError, TypeError, FileNotFoundError.
        """
        
        logging.info("Try to initialize mqtt client.")
        if self.__initialized:
            msg = "MQTTClientManager is already initialized."
            logging.error(msg)
            raise RuntimeError(msg)
        self.__initialized = True

        type_check(path, "path", str)
        if not os.path.isfile(path):
            msg = f"{path} is not a file."
            logging.error(msg)
            raise FileNotFoundError(msg)
        
        with open(path) as file:
            settings = json.load(file)

        # Refer to paho-mqtt for instruction.
        self.__CLIENT = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.__TOPIC_QUEUE: dict[str, asyncio.Queue] = {}
        try:
            self.__CLIENT.connect(
                settings["host"], 
                int(settings["port"])
            )
            self.__CLIENT.on_connect = self.__on_connect
            self.__CLIENT.on_message = self.__on_message
            self.__CLIENT.loop_start()
            # Put current event loop into userdata to call queue.put() in 
            # another thread. Does not call in __init__ because it has to in 
            # a coroutine to get the current event loop.
            self.__CLIENT.user_data_set({
                "event_loop": asyncio.get_event_loop()
            })
        except TimeoutError:
            msg = f"Fail to connect to MQTT broker {settings["host"]}:{settings["port"]}."
            logging.error(msg)
            raise TimeoutError(msg)

    def get_topic_queue(self, topic: str) -> asyncio.Queue[mqtt.MQTTMessage]:
        """Return an asyncio.Queue contains messages from a topic.

        :param topic: a topic of MQTT message.
        :raises: KeyError.
        """

        try:
            self.__TOPIC_QUEUE[topic]
            return self.__TOPIC_QUEUE[topic]
        except Exception as ex:
            logging.error(
                f"Topic \"{topic}\" is not defined or MQTTClientManager does not initialize."
            )
            raise ex

    async def handle(self, report: Report) -> None:
        pass

    def disconnect(self) -> None:
        """Disconnect the mqtt client."""
        if not self.__initialized:
            return
        self.__CLIENT.loop_stop()
        self.__CLIENT.disconnect()
        self.__initialized = False
        
    def subscribe(self, topic: str) -> None:
        """Subscribe a MQTT topic.
        
        :param topic: a MQTT topic.
        :raises: TypeError.
        """
        
        type_check(topic, "topic", str)
        self.__TOPIC_QUEUE[topic] = asyncio.Queue()
        self.__CLIENT.subscribe(topic, 2)

    async def publish(self, topic: str, payload: str) -> None:
        """ Publish a MQTT message.

        :param topic: a MQTT topic.
        :param payload: the message to be sent.
        """
        info = self.__CLIENT.publish(topic, payload)
        while not info.is_published():
            await asyncio.sleep(0)
        return
    
    def __on_connect(self, client, userdata, flags, reason_code, properties):
        """ A callback function to subscribe topics when connect/reconnect."""
        topics = list(self.__TOPIC_QUEUE.keys())
        if len(topics) > 0:
            client.subscribe([(topic, 2) for topic in topics])

    def __on_message(self, client, userdata, message):
        """ A callback function to distribute message into async queues."""
        loop = userdata["event_loop"]
        topic = message.topic
        asyncio.run_coroutine_threadsafe(
            self.__TOPIC_QUEUE[topic].put(message), 
            loop
        )
        logging.info(f"Get a MQTT message: {topic}: {message.payload.decode()}.")