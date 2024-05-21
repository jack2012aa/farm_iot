""" This test is done in Windows 11, with the help of com0com0 and ICDT Modbus RTU slave."""

import os
import asyncio
import unittest

import pandas as pd

from base.gateway.modbus import ModbusRTUGatewayManager
from base.export.common_exporters import WeeklyCsvExporter
from base.gateway.modbus import RTUConnectionSettings
from feed_scale import FeedScaleRTUSensor, FeedScaleManager


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.sensor1 = None
        self.sensor2 = None
        self.gateway_manager = ModbusRTUGatewayManager()
        self.scale_manager = FeedScaleManager()
        self.files = []

    def tearDown(self):
        self.gateway_manager = None
        self.scale_manager = None
        for file in self.files:
            if os.path.isfile(file):
                os.remove(file)

    async def test_sensor(self):

        settings = RTUConnectionSettings("COM3")
        await self.gateway_manager.create_connection(settings)

        self.sensor1 = FeedScaleRTUSensor(
            length=20,
            duration=0.1,
            waiting_time=1.0,
            name="test", 
            client=self.gateway_manager.get_connection("COM3"), 
            slave=1
        )
        self.sensor2 = FeedScaleRTUSensor(
            length=20,
            duration=0.1,
            waiting_time=1.0,
            name="test", 
            client=self.gateway_manager.get_connection("COM3"), 
            slave=2
        )
        
        df1, df2 = await asyncio.gather(self.sensor1.read_and_process(), self.sensor2.read_and_process())
        self.assertIsNotNone(df1)
        self.assertIsNotNone(df2)
        # print(df1)
        # print(df2)
        
    async def test_manager(self):
        
        path = "wrong_path.json"
        with self.assertRaises(FileNotFoundError):
            await self.scale_manager.initialize(path)
        
        path = "test/feed_scale/test_settings.json"
        await self.gateway_manager.create_connection(
            RTUConnectionSettings("COM5")
        )
        await self.scale_manager.initialize(path)
        
        async def stop_reading(task, sleeping_time = 9):
            
            await asyncio.sleep(sleeping_time)
            task.cancel()
            
        with self.assertRaises(asyncio.CancelledError):
            task1 = asyncio.create_task(self.scale_manager.run())
            task2 = asyncio.create_task(stop_reading(task1))
            await asyncio.gather(task1, task2)
        
        self.files = [
            WeeklyCsvExporter("raw")._generate_path(),
            WeeklyCsvExporter("average")._generate_path(),
            WeeklyCsvExporter("std")._generate_path(), 
            WeeklyCsvExporter("raw01")._generate_path()
        ]
        
        read = pd.read_csv(self.files[0])
        self.assertEqual(20, read.shape[0])
        read = pd.read_csv(self.files[1])
        self.assertEqual(2, read.shape[0])
        read = pd.read_csv(self.files[2])
        self.assertEqual(20, read.shape[0])

        await self.scale_manager.initialize(path)
        task1 = asyncio.create_task(self.scale_manager.run())
        stopped_task = self.scale_manager.tasks["Test 1"]
        task2 = asyncio.create_task(stop_reading(stopped_task, 5))
        task3 = asyncio.create_task(stop_reading(task1))

        with self.assertRaises(asyncio.CancelledError):
            await asyncio.gather(task1, task2, task3)

        read = pd.read_csv(self.files[0])
        self.assertEqual(30, read.shape[0])
        read = pd.read_csv(self.files[3])
        self.assertEqual(40, read.shape[0])

        path = "test/feed_scale/test_wrong_type.json"
        with self.assertRaises(ValueError):
            await self.scale_manager.initialize(path)
        
        path = "test/feed_scale/test_not_alive.json"
        with self.assertRaises(ConnectionError):
            await self.scale_manager.initialize(path)
 
        path = "test/feed_scale/test_missing_value.json"
        with self.assertRaises(KeyError):
            await self.scale_manager.initialize(path)
       


if __name__ == '__main__':
    unittest.main()