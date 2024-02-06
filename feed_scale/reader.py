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
            slave: int, 
            port: str, 
            baundrate: int = 38400, 
            bytesize: int = 8, 
            parity: str = "N", 
            stopbits: int = 1, 
            time_out: int = 5
    ) -> None:
        ''' 
        Read data from a RTU slave through a serial port. 
        * param length: number of records read in a call of `read()`.
        * param duration: the duration between two reading in each call of `read().
        * param slave: port number in modbus.
        * param port: serial port number. Should look like 'COM2', 'COM3'. Please refer to (COM and LPT) section in the Device Manager in Windows.
        * param baundrate: the baundrate of the modbus device. Please refer to the device document provided by productors. 
        * param bytesize: modbus setting. Please refer to the device document provided by productors.
        * param parity: modbus setting. Please refer to the device document provided by productors.
        * param stopbits: modbus setting. Please refer to the device document provided by productors.
        * param time_out: modbus setting. Please refer to the device document provided by productors.
        '''
        
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

    def __str__(self) -> str:
        
        descriptions = [
            "RTUReader. ", 
            f"Port: {self.PORT}", 
            f"Baundrate: {self.BAUNDRATE}", 
            f"Bytesize: {self.BYTESIZE}", 
            f"Parity: {self.PARITY}", 
            f"Stopbits: {self.STOPBITS}", 
            f"Time out: {self.TIME_OUT}", 
            f"Slave: {self._SLAVE}"
        ]
        return "\n".join(descriptions)

    async def __connect(self) -> bool:
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
    
    def __close(self) -> None:
        ''' Close connection. '''

        self.__master.close()

    async def read(self) -> pd.DataFrame:
        '''
        Read and return a batch of data. \
        Data attributes: datetime, weight
        '''

        if not await self.__connect():
            raise ConnectionError("Can not connect to RTU slave")

        time_list = []
        weight_list = []
        for _ in range(self._LENGTH_OF_A_BATCH):
            # Get register data
            data = await self.__master.read_holding_registers(address=0, count=1, slave=self._SLAVE)
            # Compute weight and append
            weight_list.append(calculate_weight_from_register(data.registers[0]))
            time_list.append(datetime.now())
            await asyncio.sleep(self._DURATION)
        self.__close()
        return pd.DataFrame({"datetime": time_list, "weight": weight_list})