import random
import unittest

from tqdm import tqdm

from base.gateway.mqtt import MQTTClientManager


class MQTTClientTestCase(unittest.IsolatedAsyncioTestCase):

    def __init__(self, methodName: str = "runTest") -> None:
        self.manager = MQTTClientManager("test/helper/mqtt_client_settings.json")
        super().__init__(methodName)

    async def asyncSetUp(self):
        await self.manager.initialize("wasted")

    async def asyncTearDown(self):
        pass

    async def test_initialize(self):

        with self.assertRaises(KeyError):
            MQTTClientManager("test/helper/mqtt_client_settings_missing_key.json")

        with self.assertRaises(FileNotFoundError):
            MQTTClientManager("test/helper/fake.json")

        with self.assertRaises(TimeoutError):
            MQTTClientManager("test/helper/mqtt_client_settings_wrong_host.json")

    async def test_read_message(self):

        self.manager.subscribe("CYC_1919test/heartbeat")
        queue = self.manager.get_topic_queue("CYC_1919test/heartbeat")
        progress_bar = tqdm(total=5, desc="Testing on read_message")
        for _ in range(5):
            message = str(random.randint(0, 2000))
            await self.manager.publish("CYC_1919test/heartbeat", message)
            recieve = await queue.get()
            self.assertEqual(message, str(recieve.payload.decode("utf-8")))
            progress_bar.update(1)
        self.manager.disconnect()
        
        
if __name__ == '__main__':
    unittest.main()