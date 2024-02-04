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

    def __init__(
            self, 
            length: int, 
            duration: float, 
            slave: int, 
            port: str, 
            baundrate: int = 38400, 
            bytesize: int = 8, 
            parity: str = "N", 
            stopbits: int = 1, 
            time_out: int = 5
    ) -> None:
        
        super().__init__(length, duration, slave)
        type_check(port, "port", str)
        type_check(baundrate, "baundrate", int)
        type_check(bytesize, "bytesize", int)
        type_check(parity, "parity", str)
        type_check(stopbits, "stopbits", int)
        type_check(time_out, "time_out", int)
        self.PORT = port
        self.BAUNDRATE = baundrate
        self.BYTESIZE = bytesize
        self.PARITY = parity
        self.STOPBITS = stopbits
        self.TIME_OUT = time_out
        self.__master = None

    async def connect(self) -> bool:
        ''' Connect to the serial gateway.'''

        self.__master = AsyncModbusSerialClient(
            port=self.PORT, 
            baudrate=self.BAUNDRATE, 
            bytesize=self.BYTESIZE, 
            parity=self.PARITY, 
            stopbits=self.STOPBITS, 
            time_out=self.TIME_OUT
        )
        return await self.__master.connect()

    async def read(self) -> pd.DataFrame:
        '''
        Read and return a batch of data. 
        Data attributes: datetime, weight
        '''

        time_list = []
        weight_list = []
        for _ in range(self._LENGTH_OF_A_BATCH):
            # Get register data
            # The program provided by Mr.Tsai read 2 coils, but I guess reading 1 is enough
            data = await self.__master.read_holding_registers(address=0, count=1, slave=self._SLAVE)
            # Compute weight and append
            weight_list.append(calculate_weight_from_register(data.registers[0]))
            time_list.append(datetime.now())
            await asyncio.sleep(self._DURATION)
        return pd.DataFrame({"datetime": time_list, "weight": weight_list})
    
    def close(self) -> None:
        ''' Close connection. '''

        self.__master.close()