import logging
import asyncio
import unittest

from test.helper import VirtualSensor
from auto_feeder_gate_manager import *
from base.gateway.mqtt import MQTTClientManager


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def __init__(self, methodName: str = "runTest") -> None:
        logging.basicConfig(level=logging.INFO)
        self.client = MQTTClientManager()
        self.virtual_sensor = VirtualSensor()
        super().__init__(methodName)

    async def asyncSetUp(self):
        self.client.disconnect()
        await self.client.initialize("test/helper/mqtt_client_settings.json")
        self.sensor = AutoFeederGateMQTTSensor(
            "test",
            "CYC_1919test", 
            "CYC_1919test/heartbeat",
            timeout=2
        )
        self.manager = AutoFeederGateManager()

    async def asyncTearDown(self):
        self.client.disconnect()
        self.sensor = None
        self.manager = None

    async def test_read_and_process(self):

        await self.virtual_sensor.publish("Open")
        await self.virtual_sensor.publish("Closed")
        await self.sensor.read_and_process()
        await self.sensor.read_and_process()
        self.assertTrue(self.sensor.is_adding_feed())

        with self.assertRaises(ValueError):
            await self.virtual_sensor.publish()
            await self.sensor.read_and_process()
        
        self.assertEqual(
            self.sensor.get_status(), 
            AutoFeederGateMQTTSensor.GateStatus.CLOSED
        )

    async def test_manager(self):
        
        # Test singleton.
        self.assertEqual(self.manager, AutoFeederGateManager())

        with self.assertRaises(FileNotFoundError):
            await self.manager.initialize("afg_manager_settings.json")
        
        with self.assertRaises(ValueError):
            await self.manager.initialize("test/helper/afg_wrong_connection_settings.json")

        with self.assertRaises(KeyError):
            await self.manager.initialize("test/helper/afg_missing_key_settings.json")

        async def manager_steps():
            # Wait for virtual sensor.
            await self.manager.initialize("test/helper/afg_manager_settings.json")
            await asyncio.sleep(2)
            await self.manager.run()
        async def send_message():
            await self.virtual_sensor.publish("Open")
            await self.virtual_sensor.publish("Closed")
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.gather(
                asyncio.wait_for(
                    self.virtual_sensor.heartbeat(1), 
                    10
                ),
                send_message(), 
                manager_steps()
            )
        gate = self.manager.get_gate("Gate 1")
        self.assertTrue(gate.is_adding_feed())

        # To Do: a test for dead sensor.


if __name__ == '__main__':
    unittest.main()