"""Read data from csv."""

import os
import asyncio

import pandas as pd

from general import type_check
from base.sensor import Sensor


class CsvSensor(Sensor):
    """Read data from csv in batch to help design filters."""

    def __init__(self, length: int, path: str, start_from: int = 0) -> None:
        """Read data from csv in batch to help design filters.
        
        :param length: length of the batch.
        :param path: path to the file.
        :param start_from: the starting row.
        :raises: TypeError, FileNotFoundError.
        """
        
        type_check(path, "path", str)
        if not os.path.isfile(path):
            raise FileNotFoundError
        self.PATH = path
        self.skip_rows = start_from
        super().__init__(length, "csv", 0.0)

    async def read_and_process(self) -> pd.DataFrame:
        """Read from the file."""

        df = pd.read_csv(
            self.PATH, 
            skiprows=range(1, self.skip_rows + 1), 
            nrows=self._LENGTH_OF_A_BATCH
        )
        self.skip_rows += self._LENGTH_OF_A_BATCH
        await asyncio.gather(
            self.notify_exporters(df), 
            self.notify_pipelines(df)
        )
        return df
    
    async def is_alive(self) -> bool:
        return True