from abc import ABC

from pymodbus.client.serial import AsyncModbusSerialClient

from base.sensor.modbus import ModbusRTUBasedSensor
from general import type_check

def calculate_weight_from_register(read: int):

    type_check(read, "read", int)
    if read > 45000:
        return (read - 65536) / 100
    return read / 100


class FeedScale(ABC):
    """An abstract class for feed scale."""


class FeedScaleRTUSensor(ModbusRTUBasedSensor, FeedScale):
    """ Read data from a RTU slave through a serial port. """

    def __init__(
            self,
            name: str, 
            client: AsyncModbusSerialClient,
            slave: int,
    ) -> None:
        """Read scale data from a RTU slave. 

        :param length: number of data read in one call of `read()`.
        :param duration: the duration between two `read_register()` in `read()`.
        :param waiting_time: the waiting time between two `read()`.
        :param client: a connection to modbus gateway. Please use `GatewayManager` to receive the connection.
        :param slave: port number in modbus.
        """
        
        type_check(client, "client", AsyncModbusSerialClient)
        super().__init__(40, 0.1, name, 19.0, client, slave)
        self.initialize_registers()
        self.initialize_data()

    def initialize_registers(self) -> None:
        reg = FeedScaleRTUSensor.ModbusRegister(
            address=0, transform=calculate_weight_from_register, field_name="Weight", function_code=3
        )
        self._registers.append(reg)