import unittest
import asyncio

from pymodbus.client import ModbusBaseClient

from base.sensor.modbus import *
from base.gateway import ModbusRTUGatewayConnectionsManager, RTUConnectionSettings
from base.manage import Manager, Report


class TestModbusSensor(ModbusRTUBasedSensor):

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
            address=2, transform=lambda x: x * 2, field_name="humidity", function_code=4
        )
        self._registers.append(reg)
    

class TestManager(Manager):

    def __init__(self) -> None:
        super().__init__()

    def handle(self, report: Report) -> None:
        print(f"Handle {report.content}")


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
            duration=0.2, 
            waiting_time=5.0,
            client=connection, 
            slave=1   
        )

        sensor2 = TestModbusSensor(
            length=10, 
            duration=0.1,
            waiting_time=5.0, 
            client=connection, 
            slave=2
        )

        df1, df2 = await asyncio.gather(self.sensor.read_and_process(), sensor2.read_and_process())
        self.assertIsNotNone(df1)
        self.assertIsNotNone(df2)
        print(df1)
        print(df2)

        sensor3 = TestModbusSensor(
            length=10, 
            duration=0.1,
            waiting_time=5.0, 
            client=connection, 
            slave=4
        )
        sensor3.set_manager(TestManager())
        empty_result = await sensor3.read_and_process()
        self.assertIsNone(empty_result)
        self.assertFalse(await sensor3.is_alive())


if __name__ == '__main__':
    unittest.main()