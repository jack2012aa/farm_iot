import os
import json
import asyncio
import logging
from abc import ABC

from pymodbus.client.serial import AsyncModbusSerialClient
from pymodbus.client.tcp import AsyncModbusTcpClient

from general import type_check
from base.manage import Report
from base.sensor import Sensor, SensorManager
from base.sensor.modbus import ModbusRTUBasedSensor, ModbusRegister, ModbusTCPBasedSensor
from base.gateway.modbus import ModbusRTUGatewayManager, ModbusTCPGatewayManager
from base.export import DataExporter
from base.export.common_exporters import ExporterFactory
from base.pipeline.time_series_filters import PipelineFactory
from auto_feeder_gate_manager import BatchConsumptionFilterBySensor


__all__ = [
    "FeedScaleRTUSensor", 
    "FeedScaleManager"
]


def calculate_weight_from_register(read: int):

    type_check(read, "read", int)
    if read > 45000:
        return (read - 65536) / 100
    return read / 100


class FeedScale(Sensor, ABC):
    """An abstract class for feed scale."""


class FeedScaleRTUSensor(ModbusRTUBasedSensor, FeedScale):
    """ Read data from a RTU slave through a serial port. """

    def __init__(
            self,
            length: int, 
            duration: float, 
            waiting_time: float, 
            name: str,
            client: AsyncModbusSerialClient,
            slave: int,
            belonging: tuple[str] = None
    ) -> None:
        """Read scale data from a RTU slave. 

        :param length: number of data read in one call of `read()`.
        :param duration: the duration between two `read_register()` in `read()`.
        :param waiting_time: the waiting time between two `read()`.
        :param name: the name of the scale.
        :param client: a connection to modbus gateway. Please use `GatewayManager` to receive the connection.
        :param slave: port number in modbus.
        :param belonging: the belonging of this sensor, who are in charge of it.
        """

        type_check(client, "client", AsyncModbusSerialClient)
        super().__init__(
            length, 
            duration, 
            name, 
            waiting_time, 
            client, 
            slave, 
            belonging
        )
        self.initialize_registers()
        self.initialize_data()

    def initialize_registers(self) -> None:
        reg = ModbusRegister(
            address=0, transform=calculate_weight_from_register, field_name=f"{self.NAME} Weight", function_code=3
        )
        self._registers.append(reg)


class FeedScaleTCPSensor(ModbusTCPBasedSensor, FeedScale):
    """ Read data from a TCP slave through a modbus tcp gateway. """

    def __init__(
            self,
            length: int, 
            duration: float, 
            waiting_time: float, 
            name: str,
            client: AsyncModbusTcpClient,
            slave: int,
            belonging: tuple[str] = None
    ) -> None:
        """Read scale data from a RTU slave. 

        :param length: number of data read in one call of `read()`.
        :param duration: the duration between two `read_register()` in `read()`.
        :param waiting_time: the waiting time between two `read()`.
        :param name: the name of the scale.
        :param client: a connection to modbus gateway. Please use `GatewayManager` to receive the connection.
        :param slave: port number in modbus.
        :param belonging: the belonging of this sensor, who are in charge of it.
        """

        type_check(client, "client", AsyncModbusTcpClient)
        super().__init__(
            length, 
            duration, 
            name, 
            waiting_time, 
            client, 
            slave, 
            belonging
        )
        self.initialize_registers()
        self.initialize_data()

    def initialize_registers(self) -> None:
        reg = ModbusRegister(
            address=0, transform=calculate_weight_from_register, field_name=f"{self.NAME} Weight", function_code=3
        )
        self._registers.append(reg)


class FeedScaleManager(SensorManager):
    """A manager class to manage different type of feed scale sensor."""

    def __init__(self) -> None:
        super().__init__()
        self.scales: list[FeedScale] = []
        self.tasks = {}

    async def handle(self, report: Report) -> None:
        
        worker = report.sign
        if isinstance(report.content, BaseException):
            if isinstance(worker, FeedScale):
                self.tasks[worker.NAME].cancel()
                logging.error(f"Stop reading from scale \"{worker.NAME}\" due to error \"{report.content}\".")
                print(f"Stop reading from scale \"{worker.NAME}\" due to error \"{report.content}\".")
                await self.send_alarm_email(worker.belonging, str(report.content))
            elif isinstance(worker, DataExporter):
                logging.error(f"Exporter \"{str(worker)}\" is broken due to \"{report.content}\".")
                print(f"Exporter \"{str(worker)}\" is broken due to \"{report.content}\".")
        #Unknown report.
        else:
            logging.warning(f"Something happen to {str(worker)}: {str(report.content)}.")
            print(f"Something happen to {str(worker)}: {str(report.content)}.")
    
        #Let other task do their job.
        await asyncio.sleep(0)
        return

    async def initialize(self, path: str, email_settings: dict = None) -> None:
        """Read settings from json file and connect to feed scales.

        The json setting file should look like this:
        {
            "Feed scale 1":{
                "connection type": "RTU", 
                "connection settings":{
                    "port":
                    "slave":
                }, 
                "length": 
                "duration":
                "waiting_time":
                "belonging": []
                "exporters":[]
                "pipelines":[]
            }
        }
        
        :param path: path to the json setting file.
        :raises: ValueError, KeyError, TypeError, FileNotFoundError.
        """
        
        if email_settings is not None:
            self.email_settings = email_settings

        type_check(path, "path", str)
        logging.info("Begin to initialize feed scales.")

        if not os.path.isfile(path):
            logging.error(f"Path \"{path}\" does not exist.")
            logging.error("Fail to initialize feed scales.")
            raise FileNotFoundError
        
        #Initialize variables
        with open(path) as file:
            configs = json.load(file)
        exporter_factory = ExporterFactory()
        pipeline_factory = PipelineFactory()
        pipeline_factory.add_filter(
            "BatchConsumptionFilterBySensor", BatchConsumptionFilterBySensor)
        
        for scale_name, settings in configs.items():
            logging.info(f"Connecting to scale \"{scale_name}\"")
            print(f"Connecting to scale \"{scale_name}\"")
            try:
                connection_type = settings["connection type"]
                match connection_type:
                    case "RTU":
                        scale = self.__create_RTU_scale(scale_name, settings)
                    case "TCP":
                        scale = self.__create_TCP_scale(scale_name, settings)
                    case _:
                        logging.error(f"Not defined connection type {connection_type} for scale \"{scale_name}\".")
                        print(f"Not defined connection type {connection_type} for scale \"{scale_name}\".")
                        raise ValueError
            except KeyError as ex:
                logging.error(f"Missing setting \"{ex.args[0]}\" when initializing scale \"{scale_name}\".")
                print(f"Missing setting \"{ex.args[0]}\" when initializing scale \"{scale_name}\".")
                raise ex
            if not await scale.is_alive():
                logging.error(f"Cannot connect to scale \"{scale_name}\".")
                print(f"Cannot connect to scale \"{scale_name}\".")
                raise ConnectionError
            
            # Create and set exporters.
            # Use get() to avoid exceptions.
            exporters = settings.get("exporters")
            if exporters is None:
                exporters = []
            for exporter in exporters:
                new_exporter = exporter_factory.create(exporter)
                new_exporter.set_manager(self)
                scale.add_exporter(new_exporter)
                
            #Create and set pipelines.
            pipelines = settings.get("pipelines")
            if pipelines is None:
                pipelines = []
            for pipeline in pipelines:
                scale.add_pipeline(pipeline_factory.create(pipeline))
                
            scale.set_manager(self)
            self.scales.append(scale)
            task = asyncio.create_task(self.__create_reading_loop(scale))
            self.tasks[scale.NAME] = task
            logging.info(f"Successfully connect to scale \"{scale_name}\".\n")
            print(f"Successfully connect to scale \"{scale_name}\".\n")
                
    def __create_RTU_scale(self, name: str, settings: dict) -> FeedScaleRTUSensor:
        """Create a rtu scale using settings."""
        
        length = settings["length"]
        duration = settings["duration"]
        waiting_time = settings["waiting_time"]
        slave = settings["connection settings"]["slave"]
        belonging = settings.get("belonging")
        if belonging is not None:
            belonging = tuple(belonging)
        
        #Get gateway connection.
        gateway_manager = ModbusRTUGatewayManager()
        port = settings["connection settings"]["port"]
        client = gateway_manager.get_connection(port)
        #Client should be connected before used.
        if client is None or not client.is_active():
            logging.error(f"RTU gateway at port {port} is not connected.")
            print(f"RTU gateway at port {port} is not connected.")
            raise ConnectionError
        
        return FeedScaleRTUSensor(
            length, duration, waiting_time, name, client, slave, belonging
        )
    
    def __create_TCP_scale(self, name: str, settings: dict) -> FeedScaleTCPSensor:
        """Create a tcp scale using settings."""
        
        length = settings["length"]
        duration = settings["duration"]
        waiting_time = settings["waiting_time"]
        slave = settings["connection settings"]["slave"]
        belonging = settings.get("belonging")
        if belonging is not None:
            belonging = tuple(belonging)
        
        #Get gateway connection.
        gateway_manager = ModbusTCPGatewayManager()
        host = settings["connection settings"]["host"]
        client = gateway_manager.get_connection(host)
        #Client should be connected before used.
        if client is None or not client.is_active():
            logging.error(f"TCP gateway at {host} is not connected.")
            print(f"TCP gateway at {host} is not connected.")
            raise ConnectionError
        
        return FeedScaleTCPSensor(
            length, duration, waiting_time, name, client, slave, belonging
        )
        
    async def __create_reading_loop(self, scale: FeedScale):
        """Create an infinite while loop of reading."""
        
        while True:
            await scale.read_and_process()
            await asyncio.sleep(scale.WAITING_TIME)
        
    async def run(self) -> None:
        """Let scales in the list begin to read in an infinite while loop."""
        
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)