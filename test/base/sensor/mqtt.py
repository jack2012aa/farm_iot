import asyncio
import logging
import unittest

from pandas import DataFrame

from test.helper import VirtualSensor
from base.gateway.mqtt import MQTTClientManager
from base.sensor.mqtt import MQTTBasedSensor


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def __init__(self, methodName: str = "runTest") -> None:
        self.client = MQTTClientManager()
        self.virtual_sensor = VirtualSensor()
        logging.basicConfig(level=logging.INFO)
        super().__init__(methodName)

    async def asyncSetUp(self):
        self.client.disconnect()
        await self.client.initialize("test/helper/mqtt_client_settings.json")
        # Create the sensor here after client initialized.
        self.sensor = MQTTBasedSensor(
            length=10, 
            waiting_time=0, 
            name="test",
            data_topic="CYC_1919test",
            heartbeat_topic="CYC_1919test/heartbeat",
            timeout=2
        )

    async def asyncTearDown(self):
        self.client.disconnect()
        self.sensor = None

    async def test_helper(self):
        topic = "CYC_1919test/heartbeat"
        self.client.subscribe(topic)
        queue = self.client.get_topic_queue(topic)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.virtual_sensor.heartbeat(), 5)
        self.assertEqual(queue.qsize(), 5)

    async def test_is_alive(self):

        results = await asyncio.gather(
            self.sensor.is_alive(), 
            asyncio.wait_for(
                self.virtual_sensor.heartbeat(1), 
                5
            ), 
            return_exceptions=True
        )
        self.assertTrue(results[0])
        self.assertIsInstance(results[1], asyncio.TimeoutError)
        async def delay_is_alive():
            await asyncio.sleep(3)
            return await self.sensor.is_alive()
        
        results = await asyncio.gather(
            delay_is_alive(), 
            asyncio.wait_for(
                self.virtual_sensor.heartbeat(10), 
                1
            ), 
            return_exceptions=True
        )
        self.assertFalse(results[0])

    async def test_read_and_process(self):

        n = 10
        results = await asyncio.gather(
            self.sensor.read_and_process(), 
            self.virtual_sensor.publish_n(n, 0)
        )
        dataframe: DataFrame = results[0]
        self.assertEqual(
            dataframe["CYC_1919test"].to_list(), 
            results[1]   
        )


if __name__ == '__main__':
    unittest.main()