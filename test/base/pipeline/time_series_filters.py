import random
import logging
import unittest
from datetime import timedelta
from collections import deque
from statistics import stdev, mean, median

from tqdm import tqdm
import pandas as pd

from general import generate_time_series
from base.pipeline.time_series_filters import *


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        logging.basicConfig(level=logging.ERROR)

    def tearDown(self):
        pass

    async def test_batch_stdev_filter(self):
        
        progress_bar = tqdm(total=5000, desc="Testing on BatchStdevFilter")
        for _ in range(2500):
            n = random.randint(1, 3)
            data = generate_time_series(number_of_columns=2, lower_bound_of_rows=1)
            filter = BatchStdevFilter(n=n, remove=False)
            result = await filter.process(data.copy())
            if (data.iloc[:, 1].dropna().size <= 2 or data.iloc[:, 2].dropna().size <= 2):
                pd.testing.assert_frame_equal(data, result)
                progress_bar.update(1)
                continue
            self.assertEqual(data.shape[1], result.shape[1])
            for i in range(data.shape[1]):
                if i == 0:
                    continue
                values = data.iloc[:, i].dropna().to_list()
                standard_deviation = stdev(values)
                average = mean(values)
                upper_bound = (average + n * standard_deviation) * 1.0001
                lower_bound = average - n * standard_deviation
                if lower_bound > 0:
                    lower_bound = lower_bound * 0.9999
                else:
                    lower_bound = lower_bound * 1.0001
                values = result.iloc[:, i].dropna().to_list()
                for value in values:
                    self.assertGreaterEqual(value, lower_bound)
                    self.assertGreaterEqual(upper_bound, value)
            progress_bar.update(1)
        
        for _ in range(2500):
            n = random.randint(1, 3)
            data = generate_time_series(number_of_columns=2, lower_bound_of_rows=1)
            filter = BatchStdevFilter(n=n, remove=True)
            result = await filter.process(data.copy())
            if (data.iloc[:, 1].dropna().size <= 2 or data.iloc[:, 2].dropna().size <= 2):
                pd.testing.assert_frame_equal(data, result)
                progress_bar.update(1)
                continue
            for i in range(data.shape[1]):
                if i == 0:
                    continue
                values = data.iloc[:, i].dropna().to_list()
                standard_deviation = stdev(values)
                average = mean(values)
                upper_bound = (average + n * standard_deviation) * 1.0001
                lower_bound = average - n * standard_deviation
                if lower_bound > 0:
                    lower_bound = lower_bound * 0.9999
                else:
                    lower_bound = lower_bound * 1.0001
                values = result.iloc[:, i].dropna().to_list()
                for value in values:
                    self.assertGreaterEqual(value, lower_bound)
                    self.assertGreaterEqual(upper_bound, value)
            progress_bar.update(1)
        progress_bar.close()

    async def test_batch_average_filter(self):

        progress_bar = tqdm(total=5000, desc="Testing on BatchAverageFilter")
        for _ in range(5000):
            data = generate_time_series(
                lower_bound_of_rows=1, 
                number_of_columns=2
            )
            filter = BatchAverageFilter()
            result = await filter.process(data.copy())
            self.assertEqual(result.shape[0], 1)
            self.assertEqual(result.iloc[0, 0], data.iloc[-1, 0])
            if (data.iloc[:, 1].dropna().size == 0 or data.iloc[:, 2].dropna().size == 0):
                row = data.iloc[0, :].to_list()
                self.assertTrue(row[1] is None or row[2] is None)
                progress_bar.update(1)
                continue
            for i in range(data.shape[1]):
                if i == 0:
                    continue
                average = mean(data.iloc[:, i].dropna().to_list())
                #Because the accuracy of float, here only check the different 
                #between average.
                self.assertTrue(
                    -0.0001 <= (average - result.iloc[0, i]) / average <= 0.0001
                )
            progress_bar.update(1)
        progress_bar.close()

    # async def test_time_filter(self):

    #     reader = CsvSensor(40, "test/base/sensor/test_data.csv")
    #     filter = TimeFilter(["2", "3"], ["Weight 1", "Weight 2"])
    #     await filter.process(await reader.read_and_process())
        
    async def test_fifo_filter(self):
        
        class TestFifoFilter(FIFOFilter):
            
            def __init__(self, length: int) -> None:
                super().__init__(length)
                
            async def process(self, data: pd.DataFrame) -> pd.DataFrame:
                
                if self._data is None:
                    self.initialize_data(data)
                
                self._replace_first_n(data)
                return self._generate_dataframe()
        
        progress_bar = tqdm(total=1000, desc="Testing on FifoFilter.")
        for _ in range(1000):
            filter = TestFifoFilter(100)
            data = generate_time_series(lower_bound_of_rows=10, upper_bound_of_rows=99, number_of_columns=2)
            result = await filter.process(data)
            self.assertEqual(data.shape[1], result.shape[1])
            for _ in range(10):
                data = generate_time_series(lower_bound_of_rows=1, upper_bound_of_rows=50, number_of_columns=2)
                new_result = await filter.process(data)
                if result.shape[0] + data.shape[0] <= 100:
                    slicing = result.shape[0]
                else:
                    slicing = 100 - data.shape[0]
                should_be = pd.concat([result.iloc[-slicing:, :], data], ignore_index=True)
                pd.testing.assert_frame_equal(should_be, new_result)
                result = new_result
            progress_bar.update(1)
        progress_bar.close()
        
    async def test_moving_average_filter(self):
        
        progress_bar = tqdm(total=5000, desc="Testing on MovingAverageFilter")
        filter = MovingAverageFilter(100)
        data_list = {1: deque([], 100), 2: deque([], 100)}
        for _ in range(5000):
            data = generate_time_series(upper_bound_of_rows=50, number_of_columns=2)
            result = await filter.process(data)
            for i in range(1, data.shape[1]):
                for raw_value, result_value in zip(
                    data.iloc[:, i].to_list(), result.iloc[:, i].to_list()
                ):
                    if pd.isna(raw_value):
                        if len(data_list[i]) == 0:
                            self.assertTrue(pd.isna(result_value))
                            continue
                        self.assertAlmostEqual(result_value, mean(data_list[i]))
                        continue
                    data_list[i].append(raw_value)
                    self.assertAlmostEqual(result_value, mean(data_list[i]))
            progress_bar.update(1)
        progress_bar.close()

    async def test_moving_median_filter(self):
        
        progress_bar = tqdm(total=5000, desc="Testing on MovingMedianFilter")
        filter = MovingMedianFilter(100)
        data_list = {1: deque([], 100), 2: deque([], 100)}
        for _ in range(5000):
            data = generate_time_series(upper_bound_of_rows=50, number_of_columns=2)
            result = await filter.process(data)
            for i in range(1, data.shape[1]):
                for raw_value, result_value in zip(
                    data.iloc[:, i].to_list(), result.iloc[:, i].to_list()
                ):
                    if pd.isna(raw_value):
                        if len(data_list[i]) == 0:
                            self.assertTrue(pd.isna(result_value))
                            continue
                        self.assertAlmostEqual(result_value, median(data_list[i]))
                        continue
                    data_list[i].append(raw_value)
                    self.assertAlmostEqual(result_value, median(data_list[i]))
            progress_bar.update(1)
        progress_bar.close()
              
    async def test_moving_stdev_filter(self):
        
        progress_bar = tqdm(total=5000, desc="Testing on MovingStdevFilter")
        n = random.randint(1, 3)
        filter = MovingStdevFilter(n, 100, False)
        previous_data = {1: deque([], 100), 2: deque([], 100)}
        for _ in range(2500):
            data = generate_time_series(lower_bound_of_rows=1, upper_bound_of_rows=50, number_of_columns=2)
            result = await filter.process(data.copy())
            for i in range(1, data.shape[1]):
                for raw_value, result_value in zip(
                    data.iloc[:, i], 
                    result.iloc[:, i]
                ):
                    if pd.isna(raw_value):
                        self.assertTrue(pd.isna(result_value))
                        continue
                    previous_data[i].append(raw_value)
                    if len(previous_data[i]) <= 2:
                        self.assertEqual(raw_value, result_value)
                        continue
                    standard_deviation = stdev(previous_data[i])
                    average = mean(previous_data[i])
                    upper_bound = (average + n * standard_deviation) * 1.0001
                    lower_bound = average - n * standard_deviation
                    if lower_bound > 0:
                        lower_bound = lower_bound * 0.9999
                    else:
                        lower_bound = lower_bound * 1.0001
                    self.assertGreaterEqual(result_value, lower_bound)
                    self.assertGreaterEqual(upper_bound, result_value)
            progress_bar.update(1)
        n = random.randint(1, 3)
        filter = MovingStdevFilter(n, 100, True)
        previous_data = {1: deque([], 100), 2: deque([], 100)}
        for _ in range(2500):
            data = generate_time_series(lower_bound_of_rows=1, upper_bound_of_rows=50, number_of_columns=2)
            result = await filter.process(data.copy())
            for i in range(1, data.shape[1]):
                for raw_value, result_value in zip(
                    data.iloc[:, i], 
                    result.iloc[:, i]
                ):
                    if pd.isna(raw_value):
                        self.assertTrue(pd.isna(result_value))
                        continue
                    previous_data[i].append(raw_value)
                    if len(previous_data[i]) <= 2:
                        self.assertEqual(raw_value, result_value)
                        continue
                    standard_deviation = stdev(previous_data[i])
                    average = mean(previous_data[i])
                    upper_bound = (average + n * standard_deviation) * 1.0001
                    lower_bound = average - n * standard_deviation
                    if lower_bound > 0:
                        lower_bound = lower_bound * 0.9999
                    else:
                        lower_bound = lower_bound * 1.0001
                    if not lower_bound <= raw_value <= upper_bound:
                        self.assertTrue(pd.isna(result_value))
            progress_bar.update(1)
        progress_bar.close()

    async def test_acculumate_filter(self):

        progress_bar = tqdm(total=4000, desc="Testing on AccumulateFilter")
        filter = AccumulateFilter(100)
        old = pd.DataFrame()
        for _ in range(200):
            for _ in range(20):
                data = generate_time_series(lower_bound_of_rows=1, upper_bound_of_rows=250)
                result = await filter.process(data.copy())
                if old.shape[0] + data.shape[0] >= 200:
                    pd.testing.assert_frame_equal(
                        data.iloc[-100:, :], 
                        result
                    )
                    old = pd.DataFrame()
                elif data.shape[0] + old.shape[0] < 100:
                    pd.testing.assert_frame_equal(
                        pd.concat([old, data], ignore_index=True), 
                        result
                    )
                    old = result
                else:
                    export_rows = 100 - old.shape[0]
                    pd.testing.assert_frame_equal(
                        pd.concat([old, data.iloc[:export_rows, :]], ignore_index=True),
                        result
                    )
                    old = pd.DataFrame(data.iloc[export_rows + 1:, :])
                progress_bar.update(1)
        progress_bar.close()

    async def test_batch_consumption_filter(self):

        progress_bar = tqdm(total=2500, desc="Testing on BatchConsumptionFilter")
        for _ in range(2500):
            filter = BatchConsumptionFilter(
                front=10, 
                tail=5, 
                methods=["difference"], 
                export_time_duration=timedelta(minutes=2)
            )
            data = generate_time_series(
                lower_bound_of_data=30, 
                upper_bound_of_data=35, 
                lower_bound_of_rows=49, 
                upper_bound_of_rows=50, 
                number_of_columns=2, 
                generate_none=False
            )
            data_deque = {
                "values0": deque([], 15), 
                "values1": deque([], 15)
            }
            datetime_deque = {
                "values0": deque([], 15),
                "values1": deque([], 15)
            }
            last_export_time = {"values0": None, "values1": None}
            remain = {
                "values0": data.iloc[0, 1], 
                "values1": data.iloc[0, 2]
            }
            lowest = remain.copy()
            result = await filter.process(data)
            index = {"values0": 0, "values1": 0}
            for key in ["values0", "values1"]:
                times = result.iloc[:, 0]
                values = result.get(key)
                seperated_result = times.to_frame().join(values).dropna()
                for (_, value), timestamp in zip(
                    data.get(key).items(), 
                    data.iloc[:, 0]
                ):
                    data_deque[key].append(value)
                    datetime_deque[key].append(timestamp)
                    lowest[key] = min(lowest[key], value)
                    if len(data_deque[key]) < 15:
                        continue
                    front_max = max(list(data_deque[key])[:10])
                    front_median = median(list(data_deque[key])[:10])
                    tail_min = min(list(data_deque[key])[-5:])
                    tail_median = median(list(data_deque[key])[-5:])
                    if tail_min >= front_max and tail_median / front_median >= 2:
                        self.assertAlmostEqual(
                            seperated_result.iloc[index[key], 1], 
                            remain[key] - lowest[key]
                        )
                        self.assertEqual(
                            seperated_result.iloc[index[key], 0], 
                            timestamp
                        )
                        remain[key] = median(list(data_deque[key])[-5:])
                        lowest[key] = remain[key]
                        last_export_time[key] = timestamp
                        index[key] += 1
                        continue
                    if last_export_time[key] is None or \
                        last_export_time[key] + timedelta(minutes=2) <= timestamp:
                        self.assertAlmostEqual(
                            seperated_result.iloc[index[key], 1], 
                            remain[key] - lowest[key]
                        )
                        self.assertEqual(
                            seperated_result.iloc[index[key], 0], 
                            timestamp
                        )
                        remain[key] = lowest[key]
                        last_export_time[key] = timestamp
                        index[key] += 1

            data = generate_time_series(
                lower_bound_of_data=80, 
                upper_bound_of_data=90, 
                lower_bound_of_rows=49, 
                upper_bound_of_rows=50, 
                generate_none=False, 
                number_of_columns=2, 
                starting_datetime=data.iloc[-1, 0]
            )
            result = await filter.process(data)
            index = {"values0": 0, "values1": 0}
            for key in ["values0", "values1"]:
                times = result.iloc[:, 0]
                values = result.get(key)
                seperated_result = times.to_frame().join(values).dropna()
                for (_, value), timestamp in zip(
                    data.get(key).items(), 
                    data.iloc[:, 0]
                ):
                    data_deque[key].append(value)
                    datetime_deque[key].append(timestamp)
                    lowest[key] = min(lowest[key], value)
                    if len(data_deque[key]) < 15:
                        continue
                    front_max = max(list(data_deque[key])[:10])
                    front_median = median(list(data_deque[key])[:10])
                    tail_min = min(list(data_deque[key])[-5:])
                    tail_median = median(list(data_deque[key])[-5:])
                    if tail_min >= front_max and tail_median / front_median >= 2:
                        self.assertAlmostEqual(
                            seperated_result.iloc[index[key], 1], 
                            remain[key] - lowest[key]
                        )
                        self.assertEqual(
                            seperated_result.iloc[index[key], 0], 
                            timestamp
                        )
                        remain[key] = median(list(data_deque[key])[-5:])
                        lowest[key] = remain[key]
                        last_export_time[key] = timestamp
                        index[key] += 1
                        continue
                    if last_export_time[key] is None or \
                        last_export_time[key] + timedelta(minutes=2) <= timestamp:
                        self.assertAlmostEqual(
                            seperated_result.iloc[index[key], 1], 
                            remain[key] - lowest[key]
                        )
                        self.assertEqual(
                            seperated_result.iloc[index[key], 0], 
                            timestamp
                        )
                        remain[key] = lowest[key]
                        last_export_time[key] = timestamp
                        index[key] += 1
            progress_bar.update(1)
            
        progress_bar.close()

        

if __name__ == '__main__':
    unittest.main()