import unittest

from base.sensor.csv import CsvSensor

class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.sensor = CsvSensor(40, "test/base/sensor/test_data.csv")

    def tearDown(self):
        self.sensor = None

    async def test_reading(self):

        df = await self.sensor.read_and_process()
        self.assertEqual(40, df.shape[0])
        print(await self.sensor.read_and_process())


if __name__ == '__main__':
    unittest.main()