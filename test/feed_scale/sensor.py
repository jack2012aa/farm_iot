''' This test is done in Windows 11, with the help of com0com0 and ICDT Modbus RTU slave.'''

import unittest
import os

import pandas as pd

from basic_sensor import Pipeline
from feed_scale.reader import FeedScaleRTUReader
from feed_scale.filter import StdFilter, BatchAverageFilter
from feed_scale.exporter import FeedScaleWeeklyCsvExporter
from feed_scale.sensor import FeedScale


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.data = pd.DataFrame(data={
            "datetime": [0, 10, 20, 30, 40, 50, 60], 
            "weight": [100, 120, 110, 100, 115, 100, 200]
        })
        self.data["weight"] = self.data["weight"].astype(float)
        self.average = 120.71
        self.standard_deviation = 35.87
        self.sensor = None

    def tearDown(self):
        self.data = None
        self.average = None
        self.standard_deviation = None

    async def test_pipeline(self):

        pipeline = Pipeline()
        pipeline.add_filter(StdFilter())
        pipeline.add_filter(BatchAverageFilter())
        result = await pipeline.run(self.data)
        # Check shape
        self.assertEqual(result.shape, (1, 2))
        # Check datetime
        self.assertEqual(result.iloc[0, 0], self.data.iloc[self.data.shape[0] - 1, 0])
        # Check average
        self.assertEqual(round(result.iloc[0, 1], 2), round((100 + 120 + 110 + 100 + 115 + 100 + 156.58) / 7, 2))

    async def test_sensor(self):

        std_filter = StdFilter()
        std_exporter = FeedScaleWeeklyCsvExporter("std")
        std_filter.add_exporter(std_exporter)
        pipeline = Pipeline()
        pipeline.add_filter(std_filter)
        reader = FeedScaleRTUReader(length=40, duration=0.2, slave=1, port="COM3")
        raw_exporter = FeedScaleWeeklyCsvExporter("raw")
        reader.add_exporter(raw_exporter)
        self.sensor = FeedScale(reader=reader, pipeline=pipeline, name="test")
        data = await self.sensor.run()
        self.assertEqual(40, data.iloc[30, 1])
        os.remove(std_exporter._generate_path())
        os.remove(raw_exporter._generate_path())
        print(self.sensor)


if __name__ == '__main__':
    unittest.main()