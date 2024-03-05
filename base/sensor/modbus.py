"""Define abstract sensor classes that read data from modbus."""
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from pandas import DataFrame
from pymodbus.client import ModbusBaseClient

from base.sensor import Sensor
from base.manage import Report
from base.gateway import ModbusRTUGatewayManager
from general import type_check

__all__ = [
    "ModbusRTUBasedSensor"    
]


class ModbusRTUBasedSensor(Sensor, ABC):
    """ An abstract class for reading data from modbus rtu.

    A period of read look like this:
    while True:
        for _ in range(self._LENGTH_OF_A_BATCH):
            self._CLIENT.read_register()
        wait(waiting_time)
    """

    @dataclass
    class ModbusRegister:
        """ A dataclass of how to read a type of data using modbus. 

        :param address: the address of target modbus register.
        :param transform: a function transforming read integer to correct value.
        :param field_name: name of target type.
        :param function_code: function code used to read modbus. Usually 3 or 4.        
        """

        address: int
        transform: callable
        field_name: str
        function_code: int

    def __init__(
            self,
            length: int,
            duration: float,
            name: str, 
            waiting_time: float,
            client: ModbusBaseClient,
            slave: int,
    ) -> None:
        """ An abstract class for reading data from modbus rtu.

        :param length: number of data read in one call of `read()`.
        :param duration: the duration between two `read_register()` in `read()`.
        :param waiting_time: the waiting time between two `read()`.
        :param client: a connection to modbus gateway. Please use `GatewayManager` to receive the connection.
        :param slave: port number in modbus.
        """

        type_check(duration, "duration", float)
        type_check(waiting_time, "waiting_time", float)
        type_check(slave, "slave", int)
        # Type check of client should be done in children class.

        self._DURATION = duration
        self.WAITING_TIME = waiting_time
        self._CLIENT = client
        self._SLAVE = slave
        self._registers: list[ModbusRTUBasedSensor.ModbusRegister] = []
        self._data: dict[str, list] = {"Timestamp":[]}
        self.__alive: bool = False
        super().__init__(length, name, waiting_time)

    @abstractmethod
    def initialize_registers(self) -> None:
        """Initialize registers to be read in `read_and_process()`"""
        return NotImplemented

    def initialize_data(self) -> None:
        """Initialize a dictionary with lists saving read data."""

        for register in self._registers:
            self._data[register.field_name] = []

    def _clear_data(self) -> None:
        """Clear the data in self.data"""
        for key in self._data.keys():
            self._data[key].clear()

    async def read_and_process(self) -> DataFrame | None:
        """Read a batch of data from registers, then export and process them.

        Data from different registers will be thrown into pipelines. 
        Please override this function to specify pipeline; or, create a new 
        `Sensor` class for different data.
        :raises: ModbusException.
        :return: read data with a timestamp. None if the sensor is down.
        """

        # Create empty list to save data.
        self._clear_data()

        # Read a batch of data.
        for i in range(self._LENGTH_OF_A_BATCH):

            # Because RTU clients are not thread safe, create a async lock 
            # here to manage the port access.
            manager = ModbusRTUGatewayManager()
            lock = manager.get_lock(self._CLIENT.comm_params.host)
            for register in self._registers:

                async def read_holding_registers():
                    async with lock:
                        return await self._CLIENT.read_holding_registers(
                            address=register.address, slave=self._SLAVE
                        )
                async def read_input_registers():
                    async with lock:
                        return await self._CLIENT.read_input_registers(
                            address=register.address, slave=self._SLAVE
                        )

                try:
                    if register.function_code == 3:
                        # Set timeout in Client to avoid bounding.
                        result = await read_holding_registers()
                    elif register.function_code == 4:
                        # Set timeout in Client to avoid bounding.
                        result = await read_input_registers()
                    else: # Ignore incorrect value.
                        pass
                except Exception as ex:
                    self.notify_manager(Report(sign=self, content=ex))
                    self.__alive = False
                    return None
                if isinstance(result, BaseException):
                    self.notify_manager(Report(sign=self, content=result))
                    self.__alive = False
                    continue
                key = register.field_name
                value = register.transform(result.registers[0])
                self._data[key].append(value)

            self._data["Timestamp"].append(datetime.now())
            # Wait for next reading.   
            await asyncio.sleep(self._DURATION)

        self.__alive = True
        batch = DataFrame(self._data)
        # Remember to handle exceptions when notifying.
        await asyncio.gather(
            self.notify_exporters(batch),
            self.notify_pipelines(batch)
        )
        return batch
    
    async def is_alive(self) -> bool:
        return self.__alive