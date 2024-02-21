import unittest
from datetime import datetime

from pandas import DataFrame
import pandas as pd

from base.pipeline import Filter, Pipeline
from base.pipeline.common_filters import *


class TestDecreaseFilter(Filter):

    def __init__(self) -> None:
        super().__init__()

    async def process(self, data: DataFrame) -> DataFrame:
        def decrease(x):
            return x - 100
        return data.map(decrease)


class TestIncreaseFilter(Filter):

    def __init__(self) -> None:
        super().__init__()

    async def process(self, data: DataFrame) -> DataFrame:
        def add(x):
            return x + 10
        return data.map(add)
    

class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.pipeline = Pipeline()

    def tearDown(self):
        pass

    async def test_pipeline(self):

        filter1 = TestDecreaseFilter()
        filter2 = TestIncreaseFilter()
        df = DataFrame([0, 10, 20, 30])
        self.pipeline.add_filter(filter1)
        self.pipeline.add_filter(filter2)
        result = await self.pipeline.run(df)
        new_df = DataFrame([-90, -80, -70, -60])
        pd.testing.assert_frame_equal(result, new_df)


if __name__ == '__main__':
    unittest.main()