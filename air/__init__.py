import os
import json
import asyncio
import logging
from abc import ABC

from pymodbus.client.tcp import AsyncModbusTcpClient

from general import type_check
from base.manage import Report
from base.sensor import Sensor, SensorManager
from base.sensor.modbus import ModbusTCPBasedSensor, ModbusRegister
from base.gateway.modbus import ModbusTCPGatewayManager
from base.export import DataExporter
from base.export.common_exporters import ExporterFactory
from base.pipeline.time_series_filters import PipelineFactory


__all__ = [
]


class FeedScale(Sensor, ABC):
    """An abstract class for feed scale."""


class AirTCPSensor(ModbusTCPBasedSensor, FeedScale):
    """ Read data from a TCP slave through a serial port. """

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
        """Read scale data from a TCP slave. 

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
        self._registers.append(ModbusRegister(
            address=2, 
            transform=lambda x: x, 
            field_name=f"{self.NAME} CO2", 
            function_code=4
        ))
        self._registers.append(ModbusRegister(
            4, 
            lambda x: x, 
            "PM2.5", 
            function_code=4
        ))
        self._registers.append(ModbusRegister(
            10, 
            lambda x: x, 
            "NH3", 
            4
        ))


class AirSensorManager(SensorManager):
    """A manager class to manage different type of feed scale sensor."""

    def __init__(self) -> None:
        super().__init__()
        self.scales: list[AirTCPSensor] = []
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
            "Air sensor 1":{
                "connection type": "TCP", 
                "connection settings":{
                    "host":
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
        logging.info("Begin to initialize air sensors.")

        if not os.path.isfile(path):
            logging.error(f"Path \"{path}\" does not exist.")
            logging.error("Fail to initialize air sensors.")
            raise FileNotFoundError
        
        #Initialize variables
        with open(path) as file:
            configs = json.load(file)
        exporter_factory = ExporterFactory()
        pipeline_factory = PipelineFactory()
        
        for air_sensor_name, settings in configs.items():
            logging.info(f"Connecting to scale \"{air_sensor_name}\"")
            print(f"Connecting to scale \"{air_sensor_name}\"")
            try:
                connection_type = settings["connection type"]
                match connection_type:
                    case "TCP":
                        scale = self.__create_TCP_sensor(air_sensor_name, settings)
                    case _:
                        logging.error(f"Not defined connection type {connection_type} for scale \"{air_sensor_name}\".")
                        print(f"Not defined connection type {connection_type} for scale \"{air_sensor_name}\".")
                        raise ValueError
            except KeyError as ex:
                logging.error(f"Missing setting \"{ex.args[0]}\" when initializing scale \"{air_sensor_name}\".")
                print(f"Missing setting \"{ex.args[0]}\" when initializing scale \"{air_sensor_name}\".")
                raise ex
            if not await scale.is_alive():
                logging.error(f"Cannot connect to scale \"{air_sensor_name}\".")
                print(f"Cannot connect to scale \"{air_sensor_name}\".")
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
            msg = f"Successfully connect to air sensor \"{air_sensor_name}\".\n"
            logging.info(msg)
            print(msg)
                
    def __create_TCP_sensor(self, name: str, settings: dict) -> AirTCPSensor:
        """Create a rtu scale using settings."""
        
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
            raise ConnectionError()
        
        return AirTCPSensor(
            length, duration, waiting_time, name, client, slave, belonging
        )
        
    async def __create_reading_loop(self, sensor: AirTCPSensor):
        """Create an infinite while loop of reading."""
        
        while True:
            await sensor.read_and_process()
            await asyncio.sleep(sensor.WAITING_TIME)
        
    async def run(self) -> None:
        """Let scales in the list begin to read in an infinite while loop."""
        
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)