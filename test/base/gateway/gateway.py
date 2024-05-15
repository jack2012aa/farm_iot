import asyncio
import unittest
import random

from tqdm import tqdm
import paho.mqtt.client as mqtt

from base.gateway import *


# class ModbusRTUGatewayTestCase(unittest.IsolatedAsyncioTestCase):

#     def setUp(self):
#         self.manager = ModbusRTUGatewayManager()

#     def tearDown(self):
#         pass

#     async def test_get_connection(self):

#         settings = RTUConnectionSettings(PORT="COM3")
#         await self.manager.create_connection(settings)
#         #Nothing should happen
#         await self.manager.create_connection(settings)

#         connection1 = self.manager.get_connection("COM3")
#         self.manager = ModbusRTUGatewayManager()
#         connection2 = self.manager.get_connection("com3")
#         self.assertEqual(connection1, connection2)

#         settings = RTUConnectionSettings(PORT="COM10")
#         with self.assertRaises(ConnectionError):
#             await self.manager.create_connection(settings)

#     async def test_lock(self):  

#         settings = RTUConnectionSettings(PORT="COM3")
#         await self.manager.create_connection(settings)
#         lock = self.manager.get_lock("com3")
#         count = []
#         async def task_test():
#             async with lock:
#                 count.append(0)
#                 await asyncio.sleep(0)
#         await asyncio.gather(task_test(), task_test())
#         self.assertEqual(len(count), 2)
        
#     async def test_initialize(self):
        
#         await self.manager.initialize("test/base/gateway/test_settings.json")
#         self.assertIsNotNone(self.manager.get_connection("COM3"))
#         self.assertIsNone(self.manager.get_connection("COM10"))
#         self.assertIsNone(self.manager.get_connection("COM7"))
#         with self.assertRaises(FileNotFoundError):
#             await self.manager.initialize("wrong_path")
            
            
class MQTTClientTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.manager = MQTTClientManager()

    def tearDown(self):
        self.manager.disconnect()

    async def test_initialize(self):
        
        await self.manager.initialize("test/base/gateway/mqtt_settings/correct_settings.json")
        self.manager.disconnect()
        with self.assertRaises(KeyError):
            await self.manager.initialize("test/base/gateway/mqtt_settings/missing_key.json")
        self.manager.disconnect()
        with self.assertRaises(FileNotFoundError):
            await self.manager.initialize("test/base/gateway/mqtt_settings/fake.json")
        self.manager.disconnect()
        with self.assertRaises(TimeoutError):
            await self.manager.initialize("test/base/gateway/mqtt_settings/wrong_host.json")
        self.manager.disconnect()
        
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
        queue = self.manager.get_topic_queue("temperature")
        progress_bar = tqdm(total=5000, desc="Testing on BatchStdevFilter")
        for _ in range(5000):
            message = str(random.randint(0, 2000))
            status = publisher.publish("temperature/test", message, 2)
            status.wait_for_publish()
            await asyncio.sleep(0.01)
            recieve = await queue.get()
            self.assertEqual(message, str(recieve.payload.decode("utf-8")))
            progress_bar.update(1)
        self.manager.disconnect()
        publisher.loop_stop()


if __name__ == '__main__':
    unittest.main()