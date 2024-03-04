import asyncio
import unittest
from datetime import datetime

from pandas import DataFrame

from base.sensor import Sensor
from base.export.common_exporters import PrintExporter


class TestSensor(Sensor):

    def __init__(self, length: int, name: str, waiting_time: float) -> None:
        super().__init__(length, name, waiting_time)

    async def read_and_process(self) -> DataFrame:
        data = {"Timestamp": [], "value": []}
        for i in range(self._LENGTH_OF_A_BATCH):
            data["Timestamp"].append(datetime.now())
            data["value"].append(i)
            await asyncio.sleep(self.WAITING_TIME)
        df = DataFrame(data)
        await self.notify_exporters(df)
        return DataFrame(df)
    
    async def is_alive(self) -> bool:
        return True


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.sensor = TestSensor(length=10, name="test", waiting_time=0.1)

    def tearDown(self):
        self.sensor = None

    async def test_read(self):

        data = await self.sensor.read_and_process()
        for i in range(10):
            self.assertEqual(i, data.iloc[i, 1])


    async def test_run(self):

        self.sensor.add_exporter(PrintExporter())
        task1 = asyncio.create_task(self.sensor.run())
        task2 = asyncio.create_task(self.sensor.run())
        
        async def cancel():
            await asyncio.sleep(5)
            task1.cancel()

        task3 = asyncio.create_task(cancel())
        with self.assertRaises(asyncio.exceptions.CancelledError):
            await asyncio.gather(task1, task2, task3)


if __name__ == '__main__':
    unittest.main()