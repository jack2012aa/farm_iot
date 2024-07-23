import logging
import asyncio
import unittest
from unittest.mock import patch, Mock

from tqdm import tqdm
import pandas as pd

from test.helper import VirtualSensor
from auto_feeder_gate_manager import *
from general import generate_time_series
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
        
    @patch("auto_feeder_gate_manager.AutoFeederGateManager")
    async def test_batch_consumption_filter_by_sensor(self, MockManager):
        
        # Declare variables.
        mock_manager: Mock # Mock AutoFeederGateManager.
        mock_gate:Mock # Mock AutoFeederGate.
        df: pd.DataFrame # Test data.
        result_df: pd.DataFrame # Processed data.
        filter: BatchConsumptionFilterBySensor # Test filter.
        remain: float # Test data remain.
        
        # Set Mock.
        mock_manager = MockManager.return_value
        mock_gate = mock_manager.get_gate.return_value
        
        filter = BatchConsumptionFilterBySensor(gate_names=["values0"])

        # First process.
        mock_gate.is_adding_feed.return_value = False
        df = generate_time_series(lower_bound_of_rows=1, generate_none=False)
        remain = min(df.get("values0").to_list())
        result_df = await filter.process(df)
        self.assertEqual(0, result_df.iloc[0, 1])
        
        progress_bar = tqdm(total=2500, desc="Testing on BatchConsumptionFilter")
        for _ in range(2500):
            progress_bar.update(1)
            # Second process.
            mock_gate.is_adding_feed.return_value = True
            df = generate_time_series(lower_bound_of_rows=1, generate_none=False)
            result_df = await filter.process(df)
            self.assertEqual(
                max(0, remain - min(df.get("values0").to_list())),
                result_df.iloc[0, 1]
            )
            remain = max(df.get("values0").to_list())

            # Third process.
            mock_gate.is_adding_feed.return_value = False
            df = generate_time_series(lower_bound_of_rows=1, generate_none=False)
            result_df = await filter.process(df)
            self.assertEqual(
                max(0, remain - min(df.get("values0").to_list())),
                result_df.iloc[0, 1]
            )
            remain = min(remain, min(df.get("values0").to_list()))
        progress_bar.close()

if __name__ == '__main__':
    unittest.main()