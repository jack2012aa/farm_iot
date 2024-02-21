import os
import abc
import aiofiles
import asyncio
from datetime import datetime

import pandas as pd

from general import type_check
from base.manage import Report
from base.export import DataExporter


class CsvExporter(DataExporter, abc.ABC):
    """ An abstract class to export data to csv."""

    def __init__(self) -> None:
        """ An abstract class to export data to csv."""
        super().__init__()

    @abc.abstractmethod
    def _generate_path(self) -> str:
        """
        Generate the path to export data. \ 
        Each sensor has its storage requirements. This method should be implemented based on those requirements.
        """
        return NotImplemented
    
    async def export(self, data: pd.DataFrame) -> None:
        """
        Export data to csv.\ 
        Initialize the csv if it has not been created.
        """

        type_check(data, "data", pd.DataFrame)
        try:
            async with asyncio.timeout(1):
                path = self._generate_path()
                if not os.path.exists(path):
                    async with aiofiles.open(path, "a", encoding="utf-8") as file:
                        await file.write(data.to_csv(
                            index=False, header=True, lineterminator="\n"
                        ))
                else:
                    async with aiofiles.open(path, "a", encoding="utf-8") as file:
                        await file.write(data.to_csv(
                            index=False, header=False, lineterminator="\n"
                        ))
        # Let the manager decides how to handle the error.
        except Exception as ex: 
            self.notify_manager(report=Report(sign=self, content=ex))


class DatabaseExporter(DataExporter, abc.ABC):
    """ An abstract class to export data to a database. """

    def __init__(self) -> None:
        """ An abstract class to export data to a database. """
        super().__init__()

    @abc.abstractmethod
    async def connect(self, config: dict) -> None:
        """
        Connect to the database. \ 
        Should be implemented based on different database systems and tables.
        """
        return NotImplemented


class WeeklyCsvExporter(CsvExporter):
    """ Save data into a csv. It will generate a new csv every week."""

    def __init__(self, file_name: str, dir: str = os.getcwd()) -> None:
        """
        Save data into a csv. It will generate a new csv every week.
        * param file_name: the final file name will be "{year}_{week}_{file_name}.csv"
        * param dir: storage directory.
        """

        type_check(dir, "dir", str)
        type_check(file_name, "file_name", str)

        if not os.path.exists(dir):
            raise ValueError(f"{dir} does not exist.")

        self.__FILE_NAME = file_name
        self.__DIR = dir
        super().__init__()

    def __str__(self) -> str:
        return f"CsvExporter. Dir: '{self.__DIR}' File name: 'year_week_{self.__FILE_NAME}'"

    def _generate_path(self) -> str:
        """ Generate path '{dir} + {year}_{week}_{file_name}.csv'"""

        now = datetime.now()

        return os.path.join(
            self.__DIR,
            f"{now.year}_{now.isocalendar()[1]}_{self.__FILE_NAME}.csv"
        )