"""Define some common exporters"""

import os
import abc
import aiofiles
import asyncio
import logging
from datetime import datetime

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.dates import ConciseDateFormatter, AutoDateLocator

from general import type_check
from base.manage import Report
from base.export import DataExporter


__all__ = [
    "CsvExporter", 
    "WeeklyCsvExporter", 
    "ExporterFactory", 
    "ScatterPlotExporter", 
    "PrintExporter"
]


class CsvExporter(DataExporter, abc.ABC):

    def __init__(
            self, 
            path: str | None = os.path.curdir, 
            file_name: str | None = "output.csv"
        ) -> None:
        """ Export data to a csv file, which directory will be 
        {path}/{file_name}.csv.

        Passing None to path or file_name means using default values.

        :param path: path to save the csv, defaults to os.path.curdir.
        :type path: str
        :param file_name: name of the csv, defaults to "output.csv."
        :type file_name: str
        """
        # Type check.
        # Enable factory use dict.get() without checking None.
        if path is not None:
            type_check(path, "path", str)
        else:
            path = os.path.curdir
        if file_name is not None:
            type_check(file_name, "file_name", str)
        else:
            file_name = "outut.csv"

        # Check the path.
        if not os.path.isdir(path):
            msg = f"Path {path} does not exist."
            logging.error(msg)
            raise NotADirectoryError(msg)

        # Initialize attributes.        
        self.PATH = path
        self.FILE_NAME = file_name
        super().__init__()
    
    async def export(self, data: pd.DataFrame) -> None:
        """ Export the data.

        If the csv exists, data will be appended to the end.

        :param data: data to export.
        :type data: pd.DataFrame
        """
        # Type check.
        type_check(data, "data", pd.DataFrame)

        # Declare variables.
        saving_directory: str # The final saving directory of the file.
        has_header: bool # Insert header or not.

        try:
            saving_directory = self._generate_path()
            # Don't need to add new header if the file is already exist.
            has_header = False if os.path.exists(saving_directory) else True
            async with aiofiles.open(saving_directory, "a", encoding="utf-8") as file:
                await file.write(data.to_csv(
                    index=False, header=has_header, lineterminator="\n"
                ))

        # Let the manager decides how to handle the error.
        except Exception as ex: 
            print(ex)
            await self.notify_manager(report=Report(sign=self, content=ex))

    def _generate_path(self) -> str:
        """Generate the path to export data."""
        return os.path.join(self.PATH, self.FILE_NAME)

class DatabaseExporter(DataExporter, abc.ABC):

    def __init__(self) -> None:
        """An abstract class to export data to a database. """
        super().__init__()

    @abc.abstractmethod
    async def connect(self, config: dict) -> None:
        """
        Connect to the database.

        Should be implemented based on different database systems and tables.
        """
        return NotImplemented


class WeeklyCsvExporter(CsvExporter):

    def __init__(
            self, 
            path: str | None = os.path.curdir, 
            file_name: str | None = "output.csv"
        ) -> None:
        """ Export data to a csv which will be created every week.

        Passing None to path or file_name means using default values. 
        The final directory will be {path}/{year}_{week}_{file_name}.csv.

        :param path: path to save the csv, defaults to os.path.curdir.
        :type path: str
        :param file_name: name of the csv, defaults to "output.csv"
        :type file_name: str, optional
        """
        #
        super().__init__(path, file_name)

    def __str__(self) -> str:

        msg = f"WeeklyCsvExporter. path: \"{self.PATH}\" "
        msg += f"File name: \"{self.FILE_NAME}\""
        return msg

    def _generate_path(self) -> str:
        """Generate path '{dir} + {year}_{week}_{file_name}.csv'"""

        # Declare variables.
        now = datetime.now()

        return os.path.join(
            self.PATH,
            f"{now.year}_{now.isocalendar()[1]}_{self.FILE_NAME}.csv"
        )
    

class PrintExporter(DataExporter):

    def __init__(self) -> None:
        """Print data in the console."""
        super().__init__()

    def __str__(self) -> str:
        return "PrintExporter"

    async def export(self, data: pd.DataFrame) -> None:
        """Print data in the console.

        :param data: data to export.
        :type data: pd.DataFrame
        """
        if data.empty:
            return
        print("PrintExporter: ", data)


class RaiseExporter(DataExporter):
    
    def __init__(self) -> None:
        """A exporter that raises an exception when receive a data."""
        super().__init__()

    async def export(self, data: pd.DataFrame) -> None:
        """ Notify the manager an AssertionError ignoring what data is.

        :param data: unused data.
        :type data: pd.DataFrame
        """
        await self.notify_manager(Report(sign=self, content=AssertionError))
        return None
    
    
class ScatterPlotExporter(DataExporter):
    
    def __init__(
            self, 
            path: str = os.path.curdir, 
            name_list: list[str] = None, 
            threshold: int = 0
        ) -> None:
        """Draw scatter plots using time series.

        The time series should be a pandas DataFrame whose first column is 
        datetime, and with one or more data column.

        :param path: path to save plots, defaults to os.path.curdir.
        :type path: str
        :param name_list: name of plots. If not provided, data name will be used, \
        defaults to None.
        :type name_list: list[str]
        :param threshold: the minimum number of data in the DataFrame needed to \
        draw the plot, defaults to 0.
        :type threshold: int
        """

        # Type check.
        type_check(path, "path", str)
        type_check(threshold, "threshold", int)
        if name_list is not None:
            if not isinstance(name_list, list):
                msg = f"name_list should be a list. Got a list of {type(name_list)} instead."
                logging.error(msg)
                raise TypeError(msg)
            for name in name_list:
                if not isinstance(name, str):
                    msg = f"name_list should be a list of str. Got a list of {type(name)} instead."
                    logging.error(msg)
                    raise TypeError(msg)

        # Initialize attributes.
        self.__PATH = path # path to save plots.
        self.__name_list = name_list # name of plots.
        self.__THRESHOLD = threshold # the minimum number of data needed.

        super().__init__()
        
    async def export(self, data: pd.DataFrame) -> None:
        """ Draw scatter plot(s).

        If the DataFrame has more than one data columns, multiple plots will be 
        created.

        :param data: data to export.
        :type data: pd.DataFrame
        """

        # Type check.        
        type_check(data, "data", pd.DataFrame)

        # Declare variables.
        datetime_list: list[datetime] # datetime list used in the x axis.
        data_list: list[int | float] # data list used in the y axis.

        # Check threshold.
        if self.__THRESHOLD > data.shape[0]:
            return

        # Draw the plot one by one.
        for i in range(data.shape[1]):
            # Use if statement so we only need to get datetime_list one time.
            if i == 0:
                datetime_list = data.iloc[:, 0].to_list()
                continue
            # to_list() will contain null values, let plt handle them.
            data_list = data.iloc[:, i].to_list()
            plt.figure(figsize=(30, 6))
            plt.scatter(datetime_list, data_list)
            plt.title(data.keys()[i])
            plt.xlabel(data.keys()[0])
            plt.ylabel(data.keys()[i])
            locator = AutoDateLocator(minticks=10, maxticks=20)
            plt.gca().xaxis.set_major_locator(locator)
            plt.gca().xaxis.set_major_formatter(ConciseDateFormatter(locator))
            plt.xticks(rotation=45)
            if self.__name_list is not None:
                file_name = f"{self.__name_list[i]}.jpg"
            else:
                file_name = f"{data.keys()[i]}.jpg"
            if os.path.isfile(os.path.join(self.__PATH, file_name)):
                i = 0
                while os.path.isfile(os.path.join(self.__PATH, f"{i}_{file_name}")):
                    i += 1
                file_name = f"{i}_{file_name}"

            plt.savefig(os.path.join(self.__PATH, file_name))
            plt.close()
            await asyncio.sleep(0)
            
        return None


class ExporterFactory():

    def __init__(self) -> None:
        """A factory class to help construct exporter using dictionary."""
        pass

    def create(self, settings: dict) -> DataExporter:
        """Create an exporter using dictionary settings.

        Settings should look like this:

        {
            "type": CSVExporter, 
            #Exporter specific settings.
        }
        
        :param settings: a dictionary with exporter specific settings.
        :raises: ValueError, KeyError.
        """

        try:
            match settings["type"]:
                case "CsvExporter":
                    return CsvExporter(
                        path=settings.get("path"), 
                        file_name=settings.get("file_name")
                    )
                case "WeeklyCsvExporter":
                    return WeeklyCsvExporter(
                        path=settings.get("path"), 
                        file_name=settings.get("file_name")
                    )
                case "PrintExporter":
                    return PrintExporter()
                case "RaiseExporter":
                    return RaiseExporter()
                case "ScatterPlotExporter":
                    return ScatterPlotExporter(
                        path=settings.get("path"), 
                        name_list=settings.get("name_list"), 
                        threshold=settings.get("threshold")
                    )
                case _:
                    msg = f"Exporter type {settings["type"]} does not exist."
                    logging.error(msg)
                    raise ValueError(msg)
        #Required specific setting not found.
        except KeyError as ex:
            msg = f"Exporter type {settings["type"]} misses setting \"{ex.args[0]}\""
            logging.error(msg)
            raise KeyError(msg)