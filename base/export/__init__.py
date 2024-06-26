import asyncio
from abc import ABC, abstractmethod

from pandas import DataFrame

from base.manage import Worker, Report
from general import type_check


class DataExporter(Worker, ABC):
    """ An abstract class to export data to different output, such as csv and database."""

    def __init__(self) -> None:
        """ An abstract class to export data to different output, such as csv and database."""
        super().__init__()

    @abstractmethod
    async def export(self, data: DataFrame) -> None:
        """ Export data to specific output."""
        return NotImplemented


class DataGenerator(Worker):
    """A class to allow subscribing exporters and exporting data.

    Please call `notify_exporters` to export data.
    """

    def __init__(self) -> None:
        """A class to allow subscribing exporters and exporting data.

        Please call `notify_exporters` to export data.
        """
        self.__exporters: list[DataExporter] = []

    def __str__(self) -> str:

        descriptions = ["DataGenerator. Exporters: "]
        descriptions = descriptions + self.list_exporters()
        # To restrict the length of the descriptions, 
        # str(DataGenerator) should not contain new line.
        return "\n".join(descriptions)

    def add_exporter(self, exporter: DataExporter) -> None:
        """Add a `DataExporter` to the exporter list.

        :param exporter: an implemented `DataExporter` object.
        :raises: TypeError.
        """

        type_check(exporter, "exporter", DataExporter)
        self.__exporters.append(exporter)

    async def notify_exporters(self, data: DataFrame) -> None:
        """Export the data throuhg all `DataExporter` objects in the exporter list.

        If the list is empty, nothing happens.
        :param data: data to be exported.
        :raises: TypeError.
        """

        type_check(data, "data", DataFrame)
        tasks = []
        for exporter in self.__exporters:
            tasks.append(
                exporter.export(data)
            )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                await self.notify_manager(Report(sign=self, content=result))

    def list_exporters(self) -> list[str]:
        """ Return the description of exporters in the exporter list."""

        descriptions = []
        for exporter in self.__exporters:
            descriptions.append(str(exporter))
        return descriptions