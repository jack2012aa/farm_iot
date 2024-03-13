import os
import unittest

from pandas import DataFrame
import pandas as pd

from base.export.common_exporters import WeeklyCsvExporter
from base.pipeline import Filter, Pipeline
from base.pipeline.time_series_filters import *


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
        
    async def test_pipeline_factoey(self):
        
        factory = PipelineFactory()
        settings = [
            {
                "type": "StdFilter"
            }, 
            {
                "type": "BatchAverageFilter", 
                "exporters": [
                    {
                        "type": "WeeklyCsvExporter", 
                        "file_name": "test", 
                        "dir": f"{os.path.curdir}"
                    }
                ]
            }
        ]
        pipeline = factory.create(settings)
        df = DataFrame({"dt": [1, 2, 3, 4, 5], "v1": [1, 2, 3, 4, 5]})
        result = await pipeline.run(df)
        self.assertEqual(3.0, result.iloc[0, 1])
        file_name = WeeklyCsvExporter("test")._generate_path()
        self.assertTrue(os.path.isfile(file_name))
        os.remove(file_name)
        
        settings.append({"type": "Wrong type"})
        with self.assertRaises(ValueError):
            factory.create(settings)
            

if __name__ == '__main__':
    unittest.main()