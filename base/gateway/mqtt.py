__all__ = [
    "MQTTClientManager"
]

import os
import json
import asyncio
import logging
import selectors

import paho.mqtt.client as mqtt

from base.gateway import GatewayManager
from base.manage import Report
from general import type_check


class MQTTClientManager(GatewayManager):
    """A class listening to MQTT broker and distributing message to 
    corresponding queue. I use paho-mqtt rather than aiomqtt due to 
    lack of support of aiomqtt on python 3.12.1.
    """

    __TOPIC_QUEUE: dict[str, asyncio.Queue] = {}
    __client = None

    def __init__(self) -> None:
        super().__init__()

    async def initialize(self, path: str) -> None:
        """ Connect to MQTT broker and start the distributor.

        The json configuration file should look like this:
        {
            "host": "192.168.1.123", 
            "port": "1883", 
            "topics": ["temperature", "humidity", "feeding_gate"]
        }

        :param path: path of configuration file.
        :raises: TypeError, FileNotFoundError.
        """

        type_check(path, "path", str)
        if not os.path.exists(path):
            logging.error(f"Path \"{path}\" does not exist.")
            logging.error("Fail to initialize MQTT Client.")
            raise FileNotFoundError
        with open(path) as file:
            settings: dict = json.load(file)

        try:
            # Define the callback function when messages arrived.
            def on_message(client, userdata, message):
                self.__TOPIC_QUEUE[message.topic].put_nowait(message)

            def on_connect(client, userdata, flags, reason_code, properties):
                topics = list(MQTTClientManager.__TOPIC_QUEUE.keys())
                if len(topics) > 0:
                    client.subscribe([(topic, 2) for topic in topics])

            # Connect to MQTT broker.
            self.__client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            self.__client.on_message = on_message
            self.__client.on_connect = on_connect
            self.__client.connect(settings["host"], int(settings["port"]))
            self.__tasks: dict[asyncio.Task] = []
            self.__tasks.append(asyncio.create_task(self.__loop_misc()))
            self.__tasks.append(asyncio.create_task(self.__loop_read()))            
        except KeyError as ex:
            logging.error(
                f"Missing key \"{ex.args[0]}\" in the configuration file of "
                + "mqtt client to initialize."
            )
            raise ex
        except TimeoutError as ex:
            logging.error(
                f"Cannot connect to mqtt host \"{settings["host"]}:"
                + f"{settings["port"]}\" when initializing MqttClientManager."
            )
            raise ex
        except Exception as ex:
            raise ex

    def get_topic_queue(self, topic: str) -> asyncio.Queue:
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

    async def disconnect(self) -> None:
        """Disconnect the mqtt client."""
        if self.__client is None:
            return
        for task in self.__tasks:
            task.cancel()
        await asyncio.sleep(0)
        
    async def __loop_misc(self) -> None:
        """Maintain client connection to the broker by calling client.loop_misc
        in a while loop.
        """
        while True:
            self.__client.loop_misc()
            await asyncio.sleep(1)
        
    async def __loop_read(self) -> None:
        """Call client.loop_read after the socket is sured to be filled with
        messages.
        """
        # ProactorEventLoop in Windows does not support add_reader().
        # Although SelectorEventLoop supports, I do not want to change the 
        # default event loop since other packages may use it.
        # selectors is a more low-level package which can do exact same 
        # thing as add_reader().
        selector = selectors.DefaultSelector()
        selector.register(self.__client.socket(), selectors.EVENT_READ)
        while True:
            events = selector.select()
            for key, mask in events:
                self.__client.loop_read()
            await asyncio.sleep(0.5)
        
    async def subscribe(self, topic: str) -> None:
        """Subscribe a MQTT topic.
        
        :param topic: a MQTT topic.
        :raises: TypeError.
        """
        
        type_check(topic, "topic", str)
        self.__TOPIC_QUEUE[topic] = asyncio.Queue()
        self.__client.subscribe(topic, 2)
        await asyncio.sleep(0)