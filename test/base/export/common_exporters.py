import os
import asyncio
import unittest
from datetime import datetime

import pandas as pd

from base.export.common_exporters import *


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exporter = None
        self.pathes = []

    def tearDown(self):
        try:
            for path in self.pathes:
                os.remove(path)
        except:
            pass
        self.exporter = None

    async def test_weekly_csv_exporter(self):

        with self.assertRaises(ValueError):
            self.exporter = WeeklyCsvExporter(
                file_name="test", 
                dir="not exist"
            )

        self.exporter = WeeklyCsvExporter("test")
        self.pathes.append(self.exporter._generate_path())
        # Initialize test data
        df = pd.DataFrame(data={"datetime": [datetime.now()], "weight": [65]})

        # Check create file successfully
        await self.exporter.export(df)
        self.assertTrue(os.path.exists(self.exporter._generate_path()))

        # Check insert data correctly
        await self.exporter.export(df)
        read = pd.read_csv(self.exporter._generate_path())
        read["datetime"] = pd.to_datetime(read["datetime"])
        df = pd.concat([df, df], ignore_index=True)
        pd.testing.assert_frame_equal(read, df)

        os.remove(self.exporter._generate_path())
        
    async def test_factory(self):
        
        factory = ExporterFactory()
        exporter = factory.create(
            {
                "type": "WeeklyCsvExporter", 
                "file_name": "test", 
                "dir": "C:/"
            }
        )
        self.assertIsInstance(exporter, WeeklyCsvExporter)
        
        with self.assertRaises(ValueError):
            factory.create({"type":"WrongType"})
            
        with self.assertRaises(KeyError):
            factory.create({"type": "WeeklyCsvExporter"})

    async def test_scatter_plot_exporter(self):
        
        exporter = ScatterPlotExporter(
            os.path.join(os.path.curdir, "test/base/export")
        )
        self.pathes.append(os.path.join(os.path.curdir, "test/base/export/values1.jpg"))
        self.pathes.append(os.path.join(os.path.curdir, "test/base/export/values2.jpg"))
        timestamp = []
        for _ in range(5):
            timestamp.append(datetime.now())
            await asyncio.sleep(2)
        values1 = [1, 2, 3, 4, 5]
        values2 = [22.3, 114.514, 1919.810, 2.14, 3.14]
        df = pd.DataFrame({"Timestamp": timestamp, "values1": values1, "values2": values2})
        await exporter.export(df)
        self.assertTrue(os.path.isfile(
            os.path.join(os.path.curdir, "test/base/export/values1.jpg")
        ))
        self.assertTrue(os.path.isfile(
            os.path.join(os.path.curdir, "test/base/export/values2.jpg")
        ))
        input("WAIT")


if __name__ == '__main__':
    unittest.main()