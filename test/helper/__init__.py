__all__ = [
    "VirtualSensor"
]

import random
import asyncio

from base.gateway.mqtt import MQTTClientManager

class VirtualSensor():

    def __init__(self, topic: str = "CYC_1919test") -> None:
        """A virtual sensor which can publish heartbeat message.

        :param topic: a random topic.
        """

        self.__client = MQTTClientManager()
        self.__TOPIC = topic

    async def heartbeat(self, duration: float = 1) -> None:
        """Publish a heartbeat message to {topic}/heartbeat every `duration` 
        second in an infinite loop.

        :param duration: duration time between two heartbeat messages in second.
        """

        while True:
            await self.__client.publish(
                self.__TOPIC + "/heartbeat", 
                "heartbeat"
            )
            await asyncio.sleep(duration)

    async def publish(self, msg: str = None) -> str:
        """Publish a message to {topic} and return it.
        
        :param msg: The message to publish. If None, publish a random message.
        """

        if msg is None:
            msg = str(random.randint(0, 1000))
        await self.__client.publish(self.__TOPIC, msg)
        return msg
    
    async def publish_n(self, n: int, duration: float) -> list[str]:
        """Publish n random messages to {topic} and return them.
        
        :param n: number of messages to publish.
        :param duration: time duration between to publish action.
        """

        results = []
        for _ in range(n):
            results.append(await self.publish())
            await asyncio.sleep(duration)
        return results