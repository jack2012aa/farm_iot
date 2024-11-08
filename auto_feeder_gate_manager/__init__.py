__all__ = [
    "AutoFeederGate",
    "AutoFeederGateMQTTSensor", 
    "AutoFeederGateManager", 
    "BatchConsumptionFilterBySensor"
]

import os
import json
import asyncio
import logging
from enum import Enum
from datetime import datetime
from abc import ABC, abstractmethod

from pandas import DataFrame

from base.manage import Report
from base.pipeline import Filter
from base.sensor import SensorManager
from general import type_check, singleton
from base.sensor.mqtt import MQTTBasedSensor
from base.export.common_exporters import ExporterFactory
from base.pipeline.time_series_filters import PipelineFactory

class AutoFeederGate(ABC):
    """An abstract class for auto feeder gate."""

    def __init__(self) -> None:
        super().__init__()

    class GateStatus(Enum):
        NO_MESSAGE = 0
        OPEN = 1
        CLOSED = 2
        MANUALLY_OPEN = 3
        MANUALLY_CLOSED = 4

    @abstractmethod
    def get_status(self) -> GateStatus:
        """Get current gate status."""
        raise NotImplemented
    
    @abstractmethod
    def is_adding_feed(self) -> bool:
        """Return whether the gate is closing."""
        raise NotImplemented

class AutoFeederGateMQTTSensor(MQTTBasedSensor, AutoFeederGate):
    """Receive gate status from mqtt.
    
    If the sensor is disconnected, read_and_process() will suspend forever. 
    Hence please use is_alive() manually and frequently.
    
    :param name: name of the `Sensor`.
    :param data_topic: the topic that sensor publish to the broker.
    :param heartbeat_topic: the topic that sensor publish heartbeat message.
    :param timeout: time to check sensor alive in second.
    :param belonging: the belonging of this sensor, who are in charge of it.
    """

    def __init__(
            self, 
            name: str, 
            data_topic: str, 
            heartbeat_topic: str, 
            timeout: float = 60, 
            belonging: tuple[str] = None
        ) -> None:
        self.__adding_feed = False
        self.__status = self.GateStatus.NO_MESSAGE
        MQTTBasedSensor.__init__(
            self,
            1, 
            name, 
            data_topic, 
            heartbeat_topic, 
            timeout, 
            belonging
        )
        AutoFeederGate.__init__(self)
    
    async def read_and_process(self) -> DataFrame:
        """ Read a batch of data from MQTT using asyncio.Queue and process in 
        pipeline.

        If receives a undefined message from mqtt, raises a ValueError.

        :raises: ValueError.
        """

        data_dict = {"Timestamp": [], self._TOPIC: []}
        data = await self._ASYNC_DATA_QUEUE.get()
        data_dict["Timestamp"].append(datetime.now())
        # Decode bytes.
        receive_status = data.payload.decode()
        data_dict[self._TOPIC].append(receive_status)
        data_frame = DataFrame(data_dict)
        match receive_status:
            case "Open":
                self.__status = self.GateStatus.OPEN
                self.__adding_feed = False
            case "Closed":
                if self.__status == self.GateStatus.OPEN:
                    self.__adding_feed = True
                self.__status = self.GateStatus.CLOSED
            case "Manually open":
                self.__status = self.GateStatus.MANUALLY_OPEN
                self.__adding_feed = False
            case "Manually closed":
                self.__status = self.GateStatus.MANUALLY_CLOSED
                self.__adding_feed = False
            case _:
                msg = f"{receive_status} is not a valid status."
                logging.error(msg)
                raise ValueError(msg)
        await asyncio.gather(
            self.notify_exporters(data_frame),
            self.notify_pipelines(data_frame)
        )
        return data_frame

    def get_status(self) -> AutoFeederGate.GateStatus:
        return self.__status
    
    def is_adding_feed(self) -> bool:
        """Return whether the gate is closing the gate. 
        
        If return True, __adding_feed is changed to False; either when the 
        sensor send new message other than "Closed", which means each closing 
        message can only be handled once.        
        """
        if self.__adding_feed:
            self.__adding_feed = False
            return True
        return self.__adding_feed
        

@singleton
class AutoFeederGateManager(SensorManager):
    """ A manager class to manage different type of feeder gate."""

    def __init__(self) -> None:
        super().__init__()
        self.__gates: dict[str: AutoFeederGate] = {}
        self.tasks = {}

    async def initialize(self, path: str) -> None:
        """ Read settings from json file, create feeder gate instances and 
        start tasks to read and process data.

        Please intialize mqtt sensors after initialized mqtt client manager.

        The json setting file should look like this:
        {
            "Gate 1": {
                "connection type": "MQTT", 
                "data_topic": ..., 
                "heartbeat_topic": ..., 
                "timeout": ..., 
                "belonging": [...]
            }, 
            "Gate 2": ...
        }
        
        :param path: path to the json setting file.
        :raises: FileNotFoundError, ValueError, KeyError, TypeError.
        """

        type_check(path, "path", str)
        logging.info("Begin to initialize auto feeder gate.")

        if not os.path.isfile(path):
            logging.error(f"Path \"{path}\" does not exist.")
            raise FileNotFoundError(f"Path \"{path}\" does not exist.")
        
        with open(path) as file:
            settings = json.load(file)
        exporter_factory = ExporterFactory()
        pipeline_factory = PipelineFactory()

        for gate_name, setting in settings.items():
            try:
                connection_type = setting["connection type"]
                match connection_type:
                    case "MQTT":
                        gate = self.__create_MQTT_gate(gate_name, setting)
                    case _:
                        msg = f"Not defined connection type {connection_type} "
                        msg += f"for auto feeder gate \"{gate_name}\"."
                        logging.error(msg)
                        raise ValueError(msg)
            except KeyError as ex:
                msg = f"Missing setting \"{ex.args[0]}\" when initializing "
                msg += f"auto feeder gate \"{gate_name}\"."
                logging.error(msg)
                raise KeyError(msg)
            
            # Use get() to avoid exceptions.
            exporters = setting.get("exporters")
            if exporters is None:
                exporters = []
            for exporter in exporters:
                new_exporter = exporter_factory.create(exporter)
                new_exporter.set_manager(self)
                gate.add_exporter(new_exporter)

            #Create and set pipelines.
            pipelines = setting.get("pipelines")
            if pipelines is None:
                pipelines = []
            for pipeline in pipelines:
                gate.add_pipeline(pipeline_factory.create(pipeline))
                
            gate.set_manager(self)
            self.__gates[gate_name] = gate
            self.tasks[gate.NAME] = asyncio.create_task(
                self.__create_reading_loop(gate)
            )
            logging.info(f"Successfully create auto feeder gate \"{gate_name}\".")

    def __create_MQTT_gate(
        self, 
        name: str, 
        settings: dict
    ) -> AutoFeederGateMQTTSensor:
        """ Create a MQTT gate using settings."""

        data_topic = settings["data_topic"]
        heartbeat_topic = settings["heartbeat_topic"]
        timeout = float(settings["timeout"])
        belonging = settings.get("belonging")
        if belonging is not None:
            belonging = tuple(belonging)

        return AutoFeederGateMQTTSensor(
            name, data_topic, heartbeat_topic, timeout, belonging
        )
    
    async def __create_reading_loop(self, gate) -> None:
        """ Create an infinite while loop of reading.
        
        :param gate: an instance of `AutoFeederGate`.
        :raises: TypeError.
        """

        while True:
            # Need to check mqtt sensor alive.
            if isinstance(gate, AutoFeederGateMQTTSensor):
                if not await gate.is_alive():
                    msg = "Cannot connect to MQTT auto feeder gate "
                    msg += f"\"{gate.NAME}\".\nTopic: {gate._TOPIC}."
                    logging.error(msg)
                    await gate.notify_manager(Report(
                        gate, asyncio.CancelledError()
                    ))
                    raise asyncio.CancelledError(msg)
            await gate.read_and_process()
        
    async def run(self) -> None:
        """Let gates in the list begin to read in an infinite while loop."""

        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
    
    def get_gate(self, name: str) -> AutoFeederGate:
        """Get a AutoFeederGate instance by name.
        
        :param name: name of the feeder gate.
        """
        type_check(name, "name", str)
        return self.__gates.get(name)

    async def handle(self, report: Report) -> None:
        pass


class BatchConsumptionFilterBySensor(Filter):

    def __init__(self, gate_names: list[str], **kwargs) -> None:
        """ Use auto gate sensor to correct remain feed and calculate feed 
        consumption between batches. 

        :param gate_names: names of auto gate sensors, which are registered \
            in AutoFeederGateManager.
        :param kwargs: used to obtain extra arguments from PipelineFactory.
        :raises: TypeError, KeyError.
        """
        # Type check.
        type_check(gate_names, "gate_names", list)
        for name in gate_names:
            if not isinstance(name, str):
                msg = f"Name in gate_names should be of type str. Got {type(name)} instead."
                logging.error(msg)
                raise TypeError(msg)

        # Initialize attributes.
        manager = AutoFeederGateManager()
        self.__remains: dict[str, float] = None # Remain feed of each feeder.
        self.__gates: dict[str, AutoFeederGate] = { # Sensor of each feeder.
            name: manager.get_gate(name) for name in gate_names
        }
        super().__init__()

    async def process(self, data: DataFrame) -> DataFrame:
        """Calculate feed consumption. Consumption = remain in last batch of 
        data - lowest weight in this batch of data.

        If gate.is_adding_feed is true, remain will be the highest weight in 
        this batch, else is the lowest weight.

        :param data: remaining weights time series.
        :raises: ValueError
        :return: a time series. Timestamp is the latest datetime in data, value 
        is the feed consumption.
        """

        # Declare variables.
        results: dict[str, list] = {} # Result dictionary.
        values: list[float] # Weights read by a feeder.

        # Initialize __remains.
        if self.__remains is None:
            self.__remains = {
                name: min(data.get(name).to_list()) for name in self.__gates.keys()
            }

        results["Timestamp"] = [data.get("Timestamp").iloc[-1]]
        # Calculate consumption one by one.
        for name in self.__remains.keys():
            values = data.get(name).to_list()
            results[name] = [max(0, self.__remains[name] - min(values))]
            if self.__gates[name].is_adding_feed():
                self.__remains[name] = max(values)
            else:
                # Prevent all value in values > remain.
                values.append(self.__remains[name])
                self.__remains[name] = min(values)
        await asyncio.sleep(0)
        return DataFrame(results)