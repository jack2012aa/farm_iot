import unittest

from pandas import DataFrame
import pandas as pd
from base.pipeline.common_filters import StdFilter

from base.pipeline.common_filters import BatchAverageFilter


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.filter = None
        data = {
            "datetime": [0, 10, 20, 30, 40, 50, 60], 
            "weight": [100, 120, 110, 100, 115, 100, 200]
        }
        self.data = DataFrame(data=data)
        self.data["weight"] = self.data["weight"].astype(float)
        self.average = 120.71
        self.standard_deviation = 35.87

    def tearDown(self):
        pass

    async def test_std_filter(self):
        
        self.filter = StdFilter()
        data = await self.filter.process(self.data)
        self.assertEqual(data.shape, (7, 2)) 
        for i in range(data.shape[0]):
            self.assertTrue(self.average - self.standard_deviation <= data.iloc[i, 1] <= self.average + self.standard_deviation)
        pd.testing.assert_series_equal(data["datetime"], self.data["datetime"])

    async def test_average_filter(self):

        self.filter = BatchAverageFilter()
        data = await self.filter.process(self.data)
        self.assertEqual(data.iloc[0, 0], self.data.iloc[self.data.shape[0] - 1, 0])
        self.assertEqual(round(data.iloc[0, 1], 2), self.average)


if __name__ == '__main__':
    unittest.main()