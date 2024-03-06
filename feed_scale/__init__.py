import os
import json
from abc import ABC

from pymodbus.client.serial import AsyncModbusSerialClient

from general import type_check
from base.manage import Manager, Report
from base.sensor.modbus import ModbusRTUBasedSensor


__all__ = [
    "FeedScaleRTUSensor", 
    "FeedScaleManager"
]


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


class FeedScaleManager(Manager):
    """A manager class to manage different type of feed scale sensor."""

    def __init__(self) -> None:
        super().__init__()

    async def handle(self, report: Report) -> None:
        pass

    async def initialize(self, path: str) -> None:
        """Read settings from json file and connect to feed scales.

        The json setting file should look like this:
        {
            "Feed scale 1":{
                "connection type": "RTU", 
                "connection settings":{
                    "port":
                    "slave:"
                    "registers":[]
                }, 
                "length": 
                "duration":
                "waiting_time":
                "exporters":[]
                "pipelines":[]
            }
        }
        
        :param path: path to the json setting file.
        :raises: ValueError, KeyError, TypeError, FileNotFoundError.
        """
        
        type_check(path, "path", str)
        
        if not os.path.isfile(path):
            print(f"Path \"{path}\" does not exist.")
            print("Fail to initialize feed scales.")
            raise FileNotFoundError
        
        with open(path) as file:
            scales = json.load(file)
        for scale_name, settings in scales.items():
            try:
                connection_type = settings["connection type"]
                length = settings["length"]
                duration = settings["duration"]
                waiting_time = settings["waiting_time"]
                match connection_type:
                    case "RTU":
                        pass
                    case _:
                        print(f"Not defined connection type {connection_type} for {scale_name}")
                        raise ValueError
            except KeyError as ex:
                print(f"Missing setting \"{ex.args[0]}\" when initializing {scale_name}")