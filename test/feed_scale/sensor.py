""" This test is done in Windows 11, with the help of com0com0 and ICDT Modbus RTU slave."""

import asyncio
import unittest

from base.gateway import ModbusRTUGatewayManager, RTUConnectionSettings
from feed_scale import FeedScaleRTUSensor


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.sensor1 = None
        self.sensor2 = None
        self.manager = None

    def tearDown(self):
        pass

    async def test_sensor(self):

        self.manager = ModbusRTUGatewayManager()
        settings = RTUConnectionSettings("COM3")
        await self.manager.create_connection(settings)

        self.sensor1 = FeedScaleRTUSensor(
            name="test", 
            client=self.manager.get_connection("COM3"), 
            slave=1
        )
        self.sensor2 = FeedScaleRTUSensor(
            name="test", 
            client=self.manager.get_connection("COM3"), 
            slave=2
        )
        
        df1, df2 = await asyncio.gather(self.sensor1.read_and_process(), self.sensor2.read_and_process())
        self.assertIsNotNone(df1)
        self.assertIsNotNone(df2)
        print(df1)
        print(df2)


if __name__ == '__main__':
    unittest.main()