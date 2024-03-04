import unittest
import asyncio

from pymodbus.client import ModbusBaseClient

from base.sensor.modbus import *
from base.gateway import ModbusRTUGatewayConnectionsManager, RTUConnectionSettings


class TestModbusSensor(ModbusBasedSensor):

    def __init__(self, length: int, duration: float, waiting_time: float, client: ModbusBaseClient, slave: int) -> None:
        super().__init__(length, duration, "test", waiting_time, client, slave)
        self.initialize_registers()
        self.initialize_data()

    def initialize_registers(self) -> None:
        reg = TestModbusSensor.ModbusRegister(
            address=0, transform=lambda x: x / 10, field_name="temp", function_code=3
        )
        self._registers.append(reg)
        reg = TestModbusSensor.ModbusRegister(
            address=1, transform=lambda x: x * 2, field_name="humidity", function_code=3
        )
        self._registers.append(reg)

    async def is_alive(self) -> bool:
        return True


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.sensor = None

    def tearDown(self):
        pass

    async def test_read(self):

        manager = ModbusRTUGatewayConnectionsManager()
        settings = RTUConnectionSettings(PORT="COM3")
        await manager.create_connection(settings)
        connection = manager.get_connection(port="COM3")

        self.sensor = TestModbusSensor(
            length=10, 
            duration=0.1, 
            waiting_time=5.0,
            client=connection, 
            slave=1   
        )
        df = await self.sensor.read_and_process()
        print(df)


if __name__ == '__main__':
    unittest.main()