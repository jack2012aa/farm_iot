import os
import json
import asyncio
import logging
from dataclasses import dataclass

from pymodbus.client.serial import AsyncModbusSerialClient

from base.manage import Manager, Report
from general import type_check


class GatewayManager(Manager):
    """An abstract manager class."""
    
    def __init__(self) -> None:
        """An abstract manager class."""
        super().__init__()


@dataclass
class RTUConnectionSettings:
    """
    :param port: serial port number. Should look like 'COM2', 'COM3'. Please refer to (COM and LPT) section in the Device Manager in Windows.
    :param baundrate: the baundrate of the modbus device. Please refer to the device document provided by productors. 
    :param bytesize: modbus setting. Please refer to the device document provided by productors.
    :param parity: modbus setting. Please refer to the device document provided by productors.
    :param stopbits: modbus setting. Please refer to the device document provided by productors.
    :param time_out: modbus setting. Please refer to the device document provided by productors.
    """

    PORT: str # port number
    BAUDRATE: int = 38400
    BYTESIZE: int = 8
    PARITY: str = "N"
    STOPBITS: int = 1
    TIME_OUT: int = 5


class ModbusRTUGatewayManager(GatewayManager):
    """A class managing connection to RTU gateways. 

    Because more than one connection to a port is not allowed, this class maps 
    each port number to a `AsyncModbusSerialClient` object. \ 
    Please use the method `get_connection` to get an client object. \ 
    DO NOT close the connection using the returned client object.
    """

    __connections: dict[str, AsyncModbusSerialClient] = {}
    __locks: dict[str, asyncio.Lock] = {}

    def __init__(self) -> None:
        super().__init__()

    def get_connection(self, port: str) -> AsyncModbusSerialClient | None:
        """
        Return a connected `AsyncModbusSerialClient` object. 
        If the connection does not exist, return None.

        This class maps port number to client object. It does not assure the 
        equality of baundrate, time out, etc. \ 
        Please use `create_connection` to create a connection.
        Please DO NOT close the connection through the returned object.
        """
        type_check(port, "port", str)
        port = port.upper()
        return ModbusRTUGatewayManager.__connections.get(port)

    def get_lock(self, port: str) -> asyncio.Lock | None:
        """Return a async lock to manage port access."""
        type_check(port, "port", str)
        port = port.upper()
        return ModbusRTUGatewayManager.__locks.get(port)

    async def create_connection(self, settings: RTUConnectionSettings) -> None:
        """
        Create an `AsyncModbusSerialClient` object.

        This class maps port number to client object. It does not assure the 
        equality of baundrate, time out, etc. \ 
        Please use `get_connection` to receive the created object.
        """

        port = settings.PORT.upper()
        if ModbusRTUGatewayManager.__connections.get(
            port
        ) is None:
            # Create a new connection.
            client = AsyncModbusSerialClient(
                    port=settings.PORT,
                    baudrate=settings.BAUDRATE,
                    bytesize=settings.BYTESIZE,
                    parity=settings.PARITY,
                    stopbits=settings.STOPBITS,
                    time_out=settings.TIME_OUT,
                )
            # Connect
            connected = await client.connect()
            if not connected:
                #Think about how to handle exceptions later.
                raise ConnectionError()
            client.params.retries = 1
            ModbusRTUGatewayManager.__connections[
                port
            ] = client
            ModbusRTUGatewayManager.__locks[
                port
            ] = asyncio.Lock()
            
    def handle(self, report: Report) -> None:
        """No one should call this method."""
        print(report.content)
        
    async def initialize(self, path: str) -> None:
        """Read settings from json file and connect to gateways.
        
        The json setting file should look like this:
        {
            "Gateway1":{
                "port": "COM3",
                "baudrate": 38400,
                "bytesize": 8,
                "parity": "N",
                "stopbits": 1,
                "time_out": 5
            },
            "Gateway2":{
                "port": COM5, 
                ...
            }
        }
        
        :param path: path to the json setting file.
        :raises: FileNotFoundError.
        """
        
        type_check(path, "path", str)
        if not os.path.exists(path):
            logging.error(f"Path \"{path}\" does not exist.")
            logging.error("Fail to initialize RTU gateways.")
            print(f"Path \"{path}\" does not exist.")
            print("Fail to initialize RTU gateways.")
            raise FileNotFoundError
        with open(path) as file:
            settings: dict = json.load(file)
        for gateway_name, setting_dict in settings.items():
            logging.info(f"Connecting to {gateway_name}...")
            print(f"Connecting to {gateway_name}...")
            try:
                setting = RTUConnectionSettings(
                    PORT=setting_dict["port"], 
                    BAUDRATE=int(setting_dict["baudrate"]),
                    BYTESIZE=int(setting_dict["bytesize"]),
                    PARITY=setting_dict["parity"], 
                    STOPBITS=int(setting_dict["stopbits"]),
                    TIME_OUT=int(setting_dict["time_out"])
                )
                await self.create_connection(settings=setting)
                logging.info("Connect successfully.")
                print("Connect successfully.\n")
            except KeyError as ex:
                logging.error(f"Missing key \"{ex.args[0]}\".")
                logging.error(f"Connection fails.\n")
                print(f"Missing key \"{ex.args[0]}\".")
                print(f"Connection fails.\n")
                continue
            except ConnectionError as ex:
                logging.error(f"Connection fails.\n")
                print(f"Connection fails.\n")
                continue