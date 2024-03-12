import unittest
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from base.sensor.csv import CsvSensor
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

    async def test_time_filter(self):

        reader = CsvSensor(40, "test/base/sensor/test_data.csv")
        filter = TimeFilter(["2", "3"], ["Weight 1", "Weight 2"])
        await filter.process(await reader.read_and_process())
        
    async def test_fifo_filter(self):
        
        class TestFifoFilter(FIFOFilter):
            
            def __init__(self, length: int) -> None:
                super().__init__(length)
                
            async def process(self, data: DataFrame) -> DataFrame:
                
                if self._data is None:
                    self.initialize_data(data)
                
                self._replace_first_n(data)
                return self._generate_dataframe()
            
        filter = TestFifoFilter(2)
        data = {
            "Timestamp": [datetime.now(), datetime.now()], 
            "value": [1, 2]
        }
        df = DataFrame(data)
        pd.testing.assert_frame_equal(
            await filter.process(df), 
            df
        )
        
        data = {
            "Timestamp": [datetime.now(), datetime.now()], 
            "value": [3, 4]
        }
        df = DataFrame(data)
        pd.testing.assert_frame_equal(
            await filter.process(df), 
            df
        )

        data = {
            "value1": deque([1, 2, 3, 4, 5, 6], maxlen=6), 
            "value2": deque([10, 20, 30, 40, 50, 60], maxlen=6)
        }
        filter = TestFifoFilter(6)
        await filter.process(DataFrame(data))
        new_rows = {
            "value1": deque([7, 8, 9], maxlen=6), 
            "value2": deque([70, 80, 90], maxlen=6)            
        }
        data["value1"].append(7)
        data["value1"].append(8)
        data["value1"].append(9)
        data["value2"].append(70)
        data["value2"].append(80)
        data["value2"].append(90)
        pd.testing.assert_frame_equal(
            await filter.process(DataFrame(new_rows)), 
            DataFrame(data)
        )
        
    async def test_moving_average_filter(self):
        
        filter = MovingAverageFilter(3)
        
        data = {
            "Timestamp": [datetime.now(), datetime.now(), datetime.now(), datetime.now(), datetime.now()],
            "values1": [1, 2, 3, 4, 5],
            "values2": [11, 22, 33, 44, 55]
        }
        
        df = await filter.process(DataFrame(data))
        data["values1"] = [1.0, 1.5, 2.0, 3.0, 4.0]
        data["values2"] = [11.0, 16.5, 22.0, 33.0, 44.0]
        pd.testing.assert_frame_equal(df, DataFrame(data))
        
    async def test_moving_median_filter(self):
        
        filter = MovingMedianFilter(6)
        data = {
            "Timestamp": [datetime.now(), datetime.now(), datetime.now(), datetime.now(), datetime.now(), datetime.now()],
            "values1": [1, 2, 2, 4, 5, 10],
            "values2": [11, 22, 33, 44, 55, 100]
        }
        df = await filter.process(DataFrame(data))
        data["values1"] = [1.0, 1.5, 2.0, 2.0, 2.0, 3.0]
        data["values2"] = [11.0, 16.5, 22.0, 27.5, 33.0, 38.5]
        pd.testing.assert_frame_equal(
            df, DataFrame(data)
        )
        
    async def test_batch_stdev_range_filter(self):
        
        filter = BatchStdevRangeFilter(1)
        data = {
            "Timestamp": [datetime.now(), datetime.now(), datetime.now(), datetime.now(), datetime.now(), datetime.now()],
            "values1": [1, 2, 2, 4, 5, 100],
            "values2": [11, 22, 33, 44, 55, 1000]
        }
        df = await filter.process(DataFrame(data))
        for values in data.values():
            values.pop()
        pd.testing.assert_frame_equal(
            df, 
            DataFrame(data),
            check_dtype=False
        )
        filter = BatchStdevRangeFilter(2)
        data = {
            "Timestamp": [datetime.now(), datetime.now(), datetime.now(), datetime.now(), datetime.now(), datetime.now()],
            "values1": [1, 2, 2, 1000, 5, 100],
            "values2": [11, 22, 33, 44, 55, 1000]
        }
        df = await filter.process(DataFrame(data))
        data["values1"] = [1, 2, 2, None, 5, 100]
        data["values2"] = [11, 22, 33, 44, 55, None]
        pd.testing.assert_frame_equal(
            DataFrame(data), 
            df, 
            check_dtype=False
        )
            
        
if __name__ == '__main__':
    unittest.main()