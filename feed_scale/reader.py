import asyncio
from datetime import datetime

import pandas as pd
from pymodbus.client.serial import AsyncModbusSerialClient

from basic_sensor import ModbusReader
from general import type_check


class FeedScaleRTUReader(ModbusReader):

    def __init__(self, length: int, duration: float, slave: int) -> None:
        super().__init__(length, duration, slave)

    async def connect(
            self,
            port: str, 
            baundrate: int = 38400, 
            bytesize: int = 8, 
            parity: str = "N", 
            stopbits: int = 1, 
            time_out: int = 5
    ) -> bool:
        ''' Connect to the serial gateway.'''

        type_check(port, "port", str)
        type_check(baundrate, "baundrate", int)
        type_check(bytesize, "bytesize", int)
        type_check(parity, "parity", str)
        type_check(stopbits, "stopbits", int)
        type_check(time_out, "time_out", int)

        self.__client = AsyncModbusSerialClient(
            port=port, 
            baudrate=baundrate, 
            bytesize=bytesize, 
            parity=parity, 
            stopbits=stopbits, 
            time_out=time_out
        )
        return await self.__client.connect()

    async def read(self) -> pd.DataFrame:
        '''
        Read and return a batch of data. 
        Data attributes: datetime, weight
        '''

        time_list = []
        weight_list = []
        for _ in range(self._LENGTH_OF_A_BATCH):
            weight_list.append(
                await self.__client.read_holding_registers(address=0, count=2, slave=self._SLAVE)
            )
            time_list.append(datetime.now())
            await asyncio.sleep(self._DURATION)
        return pd.DataFrame({"datetime": time_list, "weight": weight_list})