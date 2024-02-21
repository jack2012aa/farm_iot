import os
import unittest
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from base.export.common_exporters import WeeklyCsvExporter


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exporter = None

    def tearDown(self):
        try:
            os.remove(self.exporter._generate_path())
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


if __name__ == '__main__':
    unittest.main()