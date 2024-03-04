from dataclasses import dataclass

from pymodbus.client.serial import AsyncModbusSerialClient

from base.export import DataGenerator
from general import type_check


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
    BAUNDRATE: int = 38400
    BYTESIZE: int = 8
    PARITY: str = "N"
    STOPBITS: int = 1
    TIME_OUT: int = 5


class ModbusRTUGatewayConnectionsManager(DataGenerator):
    """A class managing connection to RTU gateways. 

    Because more than one connection to a port is not allowed, this class maps 
    each port number to a `AsyncModbusSerialClient` object. \ 
    Please use the method `get_connection` to get an client object. \ 
    DO NOT close the connection using the returned client object.
    """

    __connections: dict[str, AsyncModbusSerialClient] = {}

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
        return ModbusRTUGatewayConnectionsManager.__connections.get(port)

    async def create_connection(self, settings: RTUConnectionSettings) -> None:
        """
        Create an `AsyncModbusSerialClient` object.

        This class maps port number to client object. It does not assure the 
        equality of baundrate, time out, etc. \ 
        Please use `get_connection` to receive the created object.
        """

        if ModbusRTUGatewayConnectionsManager.__connections.get(
            settings.PORT
        ) is None:
            # Create a new connection.
            client = AsyncModbusSerialClient(
                    port=settings.PORT,
                    baudrate=settings.BAUNDRATE,
                    bytesize=settings.BYTESIZE,
                    parity=settings.PARITY,
                    stopbits=settings.STOPBITS,
                    time_out=settings.TIME_OUT, 
                )
            # Connect
            connected = await client.connect()
            client.read_holding_registers
            if not connected:
                #Think about how to handle exceptions later.
                raise ConnectionError()
            ModbusRTUGatewayConnectionsManager.__connections[
                settings.PORT
            ] = client