import asyncio
import random
import unittest
import json

from tqdm import tqdm
import paho.mqtt.client as mqtt

from base.gateway.mqtt import MQTTClientManager


class MQTTClientTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.manager = MQTTClientManager()

    def tearDown(self):
        pass

    async def test_initialize(self):

        await self.manager.initialize("test/base/gateway/mqtt_settings/correct_settings.json")
        await self.manager.disconnect()
        with self.assertRaises(KeyError):
            await self.manager.initialize("test/base/gateway/mqtt_settings/missing_key.json")
        await self.manager.disconnect()
        with self.assertRaises(FileNotFoundError):
            await self.manager.initialize("test/base/gateway/mqtt_settings/fake.json")
        await self.manager.disconnect()
        with self.assertRaises(TimeoutError):
            await self.manager.initialize("test/base/gateway/mqtt_settings/wrong_host.json")
        await self.manager.disconnect()

    async def test_read_message(self):

        publisher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        with open("test/base/gateway/mqtt_settings/correct_settings.json") as file:
            settings: dict = json.load(file)
            host = settings["host"]
            port = int(settings["port"])
        publisher.connect(host, port)
        publisher.loop_start()
        await self.manager.initialize("test/base/gateway/mqtt_settings/correct_settings.json")
        with self.assertRaises(KeyError):
            self.manager.get_topic_queue("no_topic")
        await self.manager.subscribe("temperature/test")
        queue = self.manager.get_topic_queue("temperature/test")
        progress_bar = tqdm(total=5, desc="Testing on read_message")
        for _ in range(5):
            message = str(random.randint(0, 2000))
            status = publisher.publish("temperature/test", message, 2)
            status.wait_for_publish()
            await asyncio.sleep(0.01)
            recieve = await queue.get()
            self.assertEqual(message, str(recieve.payload.decode("utf-8")))
            progress_bar.update(1)
        await self.manager.disconnect()
        publisher.loop_stop()
        
        
if __name__ == '__main__':
    unittest.main()