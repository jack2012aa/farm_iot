import asyncio
import unittest


from base.gateway.modbus import *


class ModbusRTUGatewayTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.manager = ModbusRTUGatewayManager()

    def tearDown(self):
        pass

    async def test_get_connection(self):

        settings = RTUConnectionSettings(PORT="COM3")
        await self.manager.create_connection(settings)
        #Nothing should happen
        await self.manager.create_connection(settings)

        connection1 = self.manager.get_connection("COM3")
        self.manager = ModbusRTUGatewayManager()
        connection2 = self.manager.get_connection("com3")
        self.assertEqual(connection1, connection2)

        settings = RTUConnectionSettings(PORT="COM10")
        with self.assertRaises(ConnectionError):
            await self.manager.create_connection(settings)

    async def test_lock(self):  

        settings = RTUConnectionSettings(PORT="COM3")
        await self.manager.create_connection(settings)
        lock = self.manager.get_lock("com3")
        count = []
        async def task_test():
            async with lock:
                count.append(0)
                await asyncio.sleep(0)
        await asyncio.gather(task_test(), task_test())
        self.assertEqual(len(count), 2)
        
    async def test_initialize(self):
        
        await self.manager.initialize("test/base/gateway/test_settings.json")
        self.assertIsNotNone(self.manager.get_connection("COM3"))
        self.assertIsNone(self.manager.get_connection("COM10"))
        self.assertIsNone(self.manager.get_connection("COM7"))
        with self.assertRaises(FileNotFoundError):
            await self.manager.initialize("wrong_path")
            
            
if __name__ == '__main__':
    unittest.main()