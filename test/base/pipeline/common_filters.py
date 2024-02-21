import unittest
from datetime import datetime

from pandas import DataFrame
import pandas as pd

from base.pipeline.common_filters import *


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    async def test_std_filter(self):
        
        data = DataFrame({"time": [1, 2, 3, 4, 5, 6], "value":[10.0, 20.0, 30.0 ,40.0 ,50.0 , 60.0]})
        filter = StdFilter()
        data = await filter.process(data)
        for i in range(6):
            self.assertTrue(16.0 <= data.iloc[i, 1] <= 54.0)

    async def test_batch_filter(self):

        n = datetime.now()
        data = DataFrame({"time":[datetime.now(), n , n , n, n], "value":[1.0 ,2.0 ,3.0 ,4.0 ,5.0]})
        filter = BatchAverageFilter()
        result = await filter.process(data)
        self.assertEqual(result.iloc[0, 1], 3)
        self.assertEqual(result.iloc[0, 0], n)


if __name__ == '__main__':
    unittest.main()