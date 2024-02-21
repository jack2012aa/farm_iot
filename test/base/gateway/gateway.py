import unittest

from base.gateway import RTUConnectionSettings, ModbusRTUGatewayConnectionsManager


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.manager = ModbusRTUGatewayConnectionsManager()

    def tearDown(self):
        pass

    async def test_get_connection(self):

        settings = RTUConnectionSettings(PORT="COM3")
        await self.manager.create_connection(settings)
        #Nothing should happen
        await self.manager.create_connection(settings)

        connection1 = self.manager.get_connection("COM3")
        self.manager = ModbusRTUGatewayConnectionsManager()
        connection2 = self.manager.get_connection("COM3")
        self.assertEqual(connection1, connection2)

        settings = RTUConnectionSettings(PORT="COM10")
        with self.assertRaises(ConnectionError):
            await self.manager.create_connection(settings)


if __name__ == '__main__':
    unittest.main()