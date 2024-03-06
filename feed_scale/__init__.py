import os
import json
import asyncio
from abc import ABC

from pymodbus.client.serial import AsyncModbusSerialClient

from general import type_check
from base.manage import Manager, Report
from base.sensor import Sensor
from base.sensor.modbus import ModbusRTUBasedSensor
from base.gateway import ModbusRTUGatewayManager
from base.export import DataExporter
from base.export.common_exporters import ExporterFactory
from base.pipeline.common_filters import PipelineFactory


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
    ) -> None:
        """Read scale data from a RTU slave. 

        :param length: number of data read in one call of `read()`.
        :param duration: the duration between two `read_register()` in `read()`.
        :param waiting_time: the waiting time between two `read()`.
        :param name: the name of the scale.
        :param client: a connection to modbus gateway. Please use `GatewayManager` to receive the connection.
        :param slave: port number in modbus.
        """

        type_check(client, "client", AsyncModbusSerialClient)
        super().__init__(length, duration, name, waiting_time, client, slave)
        self.initialize_registers()
        self.initialize_data()

    def initialize_registers(self) -> None:
        reg = FeedScaleRTUSensor.ModbusRegister(
            address=0, transform=calculate_weight_from_register, field_name=f"{self.NAME} Weight", function_code=3
        )
        self._registers.append(reg)


class FeedScaleManager(Manager):
    """A manager class to manage different type of feed scale sensor."""

    def __init__(self) -> None:
        super().__init__()
        self.scales: list[FeedScale] = []
        self.tasks = {}

    async def handle(self, report: Report) -> None:
        
        worker = report.sign
        if isinstance(worker, FeedScale):
            self.tasks[worker.NAME].cancel()
            print(f"Stop reading from scale \"{worker.NAME}\".")
        elif isinstance(worker, DataExporter):
            print(f"Exporter \"{str(worker)}\" is broken due to \"{report.content}\".")
            
        #Let other task do their job.
        await asyncio.sleep(0)
        return

    async def initialize(self, path: str) -> None:
        """Read settings from json file and connect to feed scales.

        The json setting file should look like this:
        {
            "Feed scale 1":{
                "connection type": "RTU", 
                "connection settings":{
                    "port":
                    "slave:"
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
        
        #Initialize variables
        with open(path) as file:
            configs = json.load(file)
        exporter_factory = ExporterFactory()
        pipeline_factory = PipelineFactory()
        
        for scale_name, settings in configs.items():
            try:
                connection_type = settings["connection type"]
                match connection_type:
                    case "RTU":
                        scale = self.__create_RTU_scale(scale_name, settings)
                    case _:
                        print(f"Not defined connection type {connection_type} for scale \"{scale_name}\".")
                        raise ValueError
            except KeyError as ex:
                print(f"Missing setting \"{ex.args[0]}\" when initializing scale \"{scale_name}\".")
                raise ex
            if not await scale.is_alive():
                print(f"Cannot connect to scale \"{scale_name}\".")
                raise ConnectionError
            
            #Create and set exporters.
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
                
    def __create_RTU_scale(self, name: str, settings: dict) -> FeedScaleRTUSensor:
        """Create a rtu scale using settings."""
        
        length = settings["length"]
        duration = settings["duration"]
        waiting_time = settings["waiting_time"]
        slave = settings["connection settings"]["slave"]
        
        #Get gateway connection.
        gateway_manager = ModbusRTUGatewayManager()
        port = settings["connection settings"]["port"]
        client = gateway_manager.get_connection(port)
        #Client should be connected before used.
        if client is None or not client.is_active():
            print(f"RTU gateway at port {port} is not connected.")
            raise ConnectionError
        
        return FeedScaleRTUSensor(
            length, duration, waiting_time, name, client, slave
        )
        
    async def __create_reading_loop(self, scale: FeedScale):
        """Create an infinite while loop of reading."""
        
        while True:
            await scale.read_and_process()
            await asyncio.sleep(scale.WAITING_TIME)
        
    async def run(self) -> None:
        """Let scales in the list begin to read in an infinite while loop."""
        
        for scale in self.scales:
            task = asyncio.create_task(self.__create_reading_loop(scale))
            self.tasks[scale.NAME] = task
            
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)