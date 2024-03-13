"""Test ways to process scale data."""
import os
import asyncio
from concurrent.futures import ProcessPoolExecutor

from base.pipeline import Pipeline
from base.pipeline.time_series_filters import *
from base.sensor.csv import CsvSensor
from base.manage import SimpleManager
from base.export.common_exporters import ScatterPlotExporter

def read(moving_length: int, stdev_range: float, batch_length:int):

    reader = CsvSensor(
        length=batch_length, 
        path="filter_design/clean_weight_data.csv", 
    )
    pipeline = Pipeline()
    filter = BatchAverageFilter()
    pipeline.add_filter(filter)
    filter = MovingAverageFilter(moving_length)
    pipeline.add_filter(filter)
    filter = BatchStdevRangeFilter()
    pipeline.add_filter(filter)
    filter = AccumulateFilter(int(120 * 120 / batch_length))
    path = f"filter_design/bstd_bavg_mavg_mstd_{moving_length}_{stdev_range}_{batch_length}"
    if not os.path.exists(path):
        os.mkdir(path)
    exporter = ScatterPlotExporter(
        save_directory=path, 
        threshold=int(120 * 120 / batch_length)
    )
    filter.add_exporter(exporter)
    # filter.add_exporter(PrintExporter())
    pipeline.add_filter(filter)
    reader.add_pipeline(pipeline)
    reader.set_manager(SimpleManager())

    async def reading_loop(reader):
        for _ in range(int(1440 * 120 / batch_length)):
            await reader.read_and_process()

    asyncio.run(reading_loop(reader))

    
def main():
    moving_lengthes = [1, 2, 3, 1, 2, 3]
    stdev_range = 1
    batch_lengthes = [120, 120, 120, 240, 240, 240]
    # read(1, 1, 120)
    with ProcessPoolExecutor() as executer:
        for moving_length, batch_length in zip(moving_lengthes, batch_lengthes):
            executer.submit(read, moving_length, stdev_range, batch_length)
    

if __name__ == "__main__":
    main()