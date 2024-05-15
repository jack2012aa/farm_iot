all = [
    "RTUConnectionSettings", 
    "ModbusRTUGatewayManager", 
    "MQTTClientManager"
]

import os
import json
import asyncio
import logging
from dataclasses import dataclass

from pymodbus.client.serial import AsyncModbusSerialClient
import paho.mqtt.client as mqtt

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
            
            
class MQTTClientManager(GatewayManager):
    """A class listening to MQTT broker and distributing message to 
    corresponding queue. I use paho-mqtt rather than aiomqtt due to 
    lack of support of aiomqtt on python 3.12.1.
    """
    
    __TOPIC_QUEUE: dict[str, asyncio.Queue] = {}
    __client = None
    
    def __init__(self) -> None:
        super().__init__()
        
    async def initialize(self, path: str) -> None:
        """Connect to MQTT broker and start the distributor.
        
        The json configuration file should look like this:
        {
            "host": "192.168.1.123", 
            "port": "1883", 
            "topics": ["temperature", "humidity", "feeding_gate"]
        }
        
        :param path: path of configuration file.
        :raises: TypeError, FileNotFoundError.
        """
        
        type_check(path, "path", str)
        if not os.path.exists(path):
            logging.error(f"Path \"{path}\" does not exist.")
            logging.error("Fail to initialize MQTT Client.")
            raise FileNotFoundError
        with open(path) as file:
            settings: dict = json.load(file)

        try:
            # Initialize topic queue.
            for topic in settings["topics"]:
                self.__TOPIC_QUEUE[topic] = asyncio.Queue()

            # Define the callback function when messages arrived.
            def on_message(client, userdata, message):
                key = message.topic.split("/")[0]
                self.__TOPIC_QUEUE[key].put_nowait(message)
                
            def on_connect(client, userdata, flags, reason_code, properties):
                client.subscribe([(topic + "/#", 2) for topic in settings["topics"]])
                
            # Connect to MQTT broker.
            self.__client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            self.__client.on_message = on_message
            self.__client.on_connect = on_connect
            self.__client.connect(settings["host"], int(settings["port"]))
            self.__client.loop_start()
        except KeyError as ex:
            logging.error(
                f"Missing key \"{ex.args[0]}\" in the configuration file of "
                + "mqtt client to initialize."
            )
            raise ex
        except TimeoutError as ex:
            logging.error(
                f"Cannot connect to mqtt host \"{settings["host"]}:"
                + f"{settings["port"]}\" when initializing MqttClientManager."
            )
            raise ex
        except Exception as ex:
            raise ex
            
    def get_topic_queue(self, topic: str) -> asyncio.Queue:
        """Return an asyncio.Queue contains messages from a topic.
        
        :param topic: a topic of MQTT message.
        :raises: KeyError.
        """
        
        try:
            self.__TOPIC_QUEUE[topic]
            return self.__TOPIC_QUEUE[topic]
        except Exception as ex:
            logging.error(
                f"Topic \"{topic}\" is not defined or MQTTClientManager does not initialize."
            )
            raise ex
        
    async def handle(self, report: Report) -> None:
        pass
    
    def disconnect(self):
        """Disconnect the mqtt client."""
        if self.__client is None:
            return
        self.__client.loop_stop()