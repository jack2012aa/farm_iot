import os
import unittest
import asyncio
from datetime import datetime

import pandas as pd

from base.export.common_exporters import WeeklyCsvExporter


class MyTestCase(unittest.TestCase):

    def setUp(self):
        self.exporter = WeeklyCsvExporter(file_name="test")

    def tearDown(self):
        os.remove(self.exporter._generate_path())
        self.exporter = None

    def test_csv_exporter(self):

        # Initialize test data
        df = pd.DataFrame(data={"datetime": [datetime.now()], "weight": [65]})

        # Check create file successfully
        asyncio.run(self.exporter.export(df))
        self.assertTrue(os.path.exists(self.exporter._generate_path()))

        # Check insert data correctly
        asyncio.run(self.exporter.export(df))
        read = pd.read_csv(self.exporter._generate_path())
        read["datetime"] = pd.to_datetime(read["datetime"])
        df = pd.concat([df, df], ignore_index=True)
        pd.testing.assert_frame_equal(read, df)

if __name__ == '__main__':
    unittest.main()