__all__ = [

]

import select
import asyncio
from datetime import datetime

from janus import Queue as AsyncQueue
import paho.mqtt.client as mqtt


class VirtualSensor():

    def __init__(self, topic: str = "CYC_1919test") -> None:
        """A virtual sensor which can publish message to test.mosquitto.org for 
        testing.

        See test.mosquitto.org

        :param topic: a random topic.
        """

        self.__client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.__TOPIC = topic

    def disconnect(self) -> None:
        """Disconnect and stop the loop of mqtt client."""
        pass

    def connect(self) -> None:
        """Connect to mqtt broker and start the loop."""
        # Use public mqtt broker for test. 
        self.__client.connect("test.mosquitto.org", 1883)

    async def heartbeat(self, duration: float = 1) -> None:
        """Publish a heartbeat message to {topic}/heartbeat every `duration` 
        second in an infinite loop.

        :param duration: duration time between two heartbeat messages in second.
        """

        while True:
            status = self.__client.publish(
                self.__TOPIC + "/heartbeat",
                "heartbeat",
                2
            )
            while not status.is_published():
                await asyncio.sleep(0.1)
            print(f"Message is published at {datetime.now()}.")
            await asyncio.sleep(duration)

    async def publish(self, length: int, duration: float = 0) -> None:
        """Publish `length` messages to {topic}. Each message will seperate by 
        `duration` second of time.  

        :param length: number of messages to publish.
        :param duration: duration time between to messages in second.
        """

        for _ in range(length):
            status = self.__client.publish(self.__TOPIC, "virtual sensor", 2)
            await asyncio.sleep(1)
            status.wait_for_publish()
            await asyncio.sleep(duration)

    async def _loop(self) -> None:
        while True:
            try:
                read, write, exception = select.select(
                    [self.__client.socket()], 
                    [self.__client.socket()] if self.__client.want_write() else [], 
                    [], 
                    0.1
                )
            except TimeoutError:
                print("Timeout")
            if self.__client.socket() in read:
                self.__client.loop_read()
            if self.__client.socket() in write:
                self.__client.loop_write()
            self.__client.loop_misc()
            await asyncio.sleep(0)