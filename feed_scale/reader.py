import asyncio
from datetime import datetime

import pandas as pd
from pymodbus.client.serial import AsyncModbusSerialClient

from basic_sensor import ModbusReader
from general import type_check

def calculate_weight_from_register(read: int):

    type_check(read, "read", int)
    if read > 45000:
        return (read - 65536) / 100
    return read / 100


class FeedScaleRTUReader(ModbusReader):
    ''' Read data from a RTU slave through a serial port. '''

    def __init__(
            self, 
            length: int, 
            duration: float, 
            client: AsyncModbusSerialClient, 
            slave: int, 
    ) -> None:
        ''' 
        Read data from a RTU slave through a serial port. 
        * param length: number of records read in a call of `read()`.
        * param duration: the duration between two reading in each call of `read().
        * param client: a connection to a modbus rtu gateway. Please receive through `GatewayManager`.
        * param slave: port number in modbus.
        '''
        
        type_check(client, "client", AsyncModbusSerialClient)
        super().__init__(length, duration, client, slave)

    async def read(self) -> pd.DataFrame:
        '''
        Read and return a batch of data. \
        Data attributes: datetime, weight
        把group問題移到SensorManager?
        '''

        time_list = []
        weight_list = []
        for _ in range(self._LENGTH_OF_A_BATCH):
            # Get register data
            data = await self._CLIENT.read_holding_registers(address=0, count=1, slave=self._SLAVE)
            # Compute weight and append
            weight_list.append(calculate_weight_from_register(data.registers[0]))
            time_list.append(datetime.now())
            await asyncio.sleep(self._DURATION)

        return pd.DataFrame({"datetime": time_list, "weight": weight_list})