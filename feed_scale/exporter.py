import aiofiles
from datetime import datetime
import os

from pandas import DataFrame

from basic_sensor import CsvExporter
from general import type_check


class FeedScaleWeeklyCsvExporter(CsvExporter):
    ''' Save weight records into a csv. It will generate a new csv every week.'''

    def __init__(self, file_name: str, dir: str = os.getcwd()) -> None:
        '''
        Save weight records into a csv. It will generate a new csv every week.
        * param file_name: the final file name will be "{year}_{week}_{file_name}.csv"
        * param dir: storage directory.
        '''

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
        ''' Generate path "{dir} + {year}_{week}_{file_name}.csv"'''

        now = datetime.now()

        return os.path.join(
            self.__DIR, 
            f"{now.year}_{now.isocalendar()[1]}_{self.__FILE_NAME}.csv"
        )
    
    async def export(self, data: DataFrame) -> None:
        '''
        Export data to csv.\ 
        Initialize the csv if it has not been created.
        '''

        type_check(data, "data", DataFrame)
        path = self._generate_path()
        if not os.path.exists(path):
            async with aiofiles.open(path, "a", encoding="utf-8") as file:
                await file.write(data.to_csv(index=False, header=True, lineterminator="\n"))
        else:
            async with aiofiles.open(path, "a", encoding="utf-8") as file:
                await file.write(data.to_csv(index=False, header=False, lineterminator="\n"))