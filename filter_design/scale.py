"""Test ways to process scale data."""
import os
import asyncio
import multiprocessing
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

from base.pipeline import Pipeline
from base.pipeline.common_filters import *
from base.sensor.csv import CsvSensor
from base.export.common_exporters import ScatterPlotExporter

def read(moving_length: int, stdev_range: float, batch_length:int, batch_start: int):

    reader = CsvSensor(
        batch_length, 
        os.path.join(os.path.curdir, "filter_design/clean_weight_data.csv"), 
        batch_start
    )
    pipeline = Pipeline()
    filter = MovingAverageFilter(moving_length)
    pipeline.add_filter(filter)
    filter = MovingStdevRangeFilter(stdev_range, moving_length)
    exporter = ScatterPlotExporter(
        save_directory="filter_design", 
        name_list=[
            f"Weight1_{batch_start}_{moving_length}_{stdev_range}", 
            f"Weight2_{batch_start}_{moving_length}_{stdev_range}", 
        ]
    )
    filter.add_exporter(exporter)
    pipeline.add_filter(filter)
    reader.add_pipeline(pipeline)
    asyncio.run(reader.read_and_process())
    
def main():
    lengthes = [
        80, 80, 120, 160
    ]
    stdev_ranges = [
        1, 1.5, 2, 2.5
    ]
    with ProcessPoolExecutor() as executor:
        for length, stdev_range in zip(lengthes, stdev_ranges):
            for i in range(4000, 123835, 4000):
                executor.submit(read, length, stdev_range, 4000, i)
    

if __name__ == "__main__":
    asyncio.run(main())