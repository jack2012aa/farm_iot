""" This test is done in Windows 11, with the help of com0com0 and ICDT Modbus RTU slave."""

import os
import asyncio
import unittest

import pandas as pd

from base.gateway import ModbusRTUGatewayManager, RTUConnectionSettings
from base.export.common_exporters import WeeklyCsvExporter
from feed_scale import FeedScaleRTUSensor, FeedScaleManager


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.sensor1 = None
        self.sensor2 = None
        self.gateway_manager = ModbusRTUGatewayManager()
        self.scale_manager = FeedScaleManager()

    def tearDown(self):
        self.gateway_manager = None
        self.scale_manager = None

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
        scale = self.scale_manager.scales[0]
        await scale.read_and_process()
        
        pathes = [
            WeeklyCsvExporter("raw")._generate_path(),
            WeeklyCsvExporter("average")._generate_path(),
            WeeklyCsvExporter("std")._generate_path()
        ]
        
        read = pd.read_csv(pathes[0])
        self.assertEqual(40, read.shape[0])
        read = pd.read_csv(pathes[1])
        self.assertEqual(1, read.shape[0])
        read = pd.read_csv(pathes[2])
        self.assertEqual(40, read.shape[0])
        
        for p in pathes:
            self.assertTrue(os.path.isfile(p))
            os.remove(p)
            
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