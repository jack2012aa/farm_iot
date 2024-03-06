"""Define data filtering/processing interfaces."""

from abc import ABC, abstractmethod

from pandas import DataFrame

from base.export import DataGenerator
from general import type_check


class Filter(DataGenerator, ABC):
    """A class to process `DataFrame`. It should be used with a `Pipeline` object."""

    def __init__(self) -> None:
        """A class to process `DataFrame`. It should be used with a `Pipeline` object."""
        super().__init__()

    @abstractmethod
    async def process(self, data: DataFrame) -> DataFrame:
        """Process and return the data.
        
        Remember to call `self.notify_exporters` if needed.
        """
        return NotImplemented


class Pipeline:
    """A pipeline of `Filter`s. 
    
    Since subscribing multiple `Pipeline` is allowed for `Sensor` objects, 
    use `DataExporter` to export pipeline's result correctly.
    """

    def __init__(self) -> None:
        """ A pipeline of `Filter`s. """
        self.__filters: list[Filter] = []

    def __str__(self) -> str:

        descriptions = ["Pipeline. Filters: "]
        for filter in self.__filters:
            descriptions.append(f"{type(filter)} with exporters {";".join(filter.list_exporters())}")
        return "\n".join(descriptions)

    def add_filter(self, filter: Filter):
        """Add a `Filter` object to the filter list. """

        type_check(filter, "filter", Filter)
        self.__filters.append(filter)

    async def run(self, data: DataFrame) -> DataFrame:
        """ Call every filter in the filter list and notify exporters. """

        type_check(data, "data", DataFrame)
        for filter in self.__filters:
            data = await filter.process(data)
            await filter.notify_exporters(data)
        return data

    def list_filters(self) -> list[str]:
        """ Return the description of filters in the filter list."""

        descriptions = []
        for filter in self.__filters:
            descriptions.append(str(filter))
        return descriptions