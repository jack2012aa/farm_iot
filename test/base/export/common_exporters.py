import os
import shutil
import asyncio
import unittest
from datetime import datetime

import pandas as pd

from general import generate_time_series
from base.export.common_exporters import *


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exporter = None
        self.PATH = "test/helper/trash_files" # path containig every created files.

    def tearDown(self):
        # Delete every file in the folder.
        for filename in os.listdir(self.PATH):
            file_path = os.path.join(self.PATH, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        self.exporter = None

    async def test_csv_exporter(self):

        # Declare variables.
        data_dict: dict[str, list] # Data to create DataFrame.
        df: pd.DataFrame # Testing data.
        file_name: str # Testing file name.
        read_df: pd.DataFrame # Read exported data.
        
        # First export.
        file_name = "test1"
        self.exporter = CsvExporter(path=self.PATH, file_name=file_name)
        self.assertEqual(
            self.exporter._generate_path(), 
            os.path.join(self.PATH, file_name)
        )
        data_dict = {
            "a": [1, 2, 3, 4], 
            "b": [10, 20, 30, 40]
        }
        df = pd.DataFrame(data_dict)
        await self.exporter.export(df)
        read_df = pd.read_csv(self.exporter._generate_path())
        pd.testing.assert_frame_equal(df, read_df)

        # Second export.
        data_dict = {
            "a": [1, 2], 
            "b": [30, 40]
        }
        df = pd.DataFrame(data_dict)
        await self.exporter.export(df)
        data_dict = {
            "a": [1, 2, 3, 4, 1, 2], 
            "b": [10, 20, 30, 40, 30, 40]
        }
        df = pd.DataFrame(data_dict)
        read_df = pd.read_csv(self.exporter._generate_path())
        pd.testing.assert_frame_equal(df, read_df)

    async def test_weekly_csv_exporter(self):

        # Declare variables.
        df: pd.DataFrame # Test data.
        now: datetime # Current datetime.
        file_name: str # File name.
        read_df: pd.DataFrame # Read exported data.

        with self.assertRaises(NotADirectoryError):
            self.exporter = WeeklyCsvExporter(
                file_name="test", 
                path="not exist"
            )

        file_name = "test"
        self.exporter = WeeklyCsvExporter(path=self.PATH, file_name=file_name)

        # Check file directory.
        now = datetime.now()
        self.assertEqual(
            self.exporter._generate_path(),
            os.path.join(
                self.PATH, 
                f"{now.year}_{now.isocalendar()[1]}_{file_name}.csv"
            )
        )

        df = generate_time_series()
        # Check create file successfully.
        await self.exporter.export(df)
        self.assertTrue(os.path.isfile(self.exporter._generate_path()))

        # Check insert data correctly
        await self.exporter.export(df)
        read_df = pd.read_csv(self.exporter._generate_path())
        read_df["Timestamp"] = pd.to_datetime(read_df["Timestamp"])
        df = pd.concat([df, df], ignore_index=True)
        pd.testing.assert_frame_equal(read_df, df)
        
    async def test_factory(self):
        
        factory = ExporterFactory()
        exporter = factory.create(
            {
                "type": "WeeklyCsvExporter", 
                "file_name": "test", 
                "path": "C:/"
            }
        )
        self.assertIsInstance(exporter, WeeklyCsvExporter)
        
        with self.assertRaises(ValueError):
            factory.create({"type":"WrongType"})

    async def test_scatter_plot_exporter(self):
        
        # Declare variables.
        df: pd.DataFrame # Test data.

        self.exporter = ScatterPlotExporter(path=self.PATH)
        df = generate_time_series(number_of_columns=2)
        await self.exporter.export(df)
        self.assertTrue(os.path.isfile(
            os.path.join(self.PATH, f"values0.jpg")
        ))
        self.assertTrue(os.path.isfile(
            os.path.join(self.PATH, f"values1.jpg")
        ))
        input("WAIT")


if __name__ == '__main__':
    unittest.main()