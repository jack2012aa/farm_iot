''' This test is done in Windows 11, with the help of com0com0 and ICDT Modbus RTU slave.'''

import unittest
from feed_scale.reader import FeedScaleRTUReader


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.reader = FeedScaleRTUReader(length=40, duration=0.2, slave=1, port="COM3")

    def tearDown(self):
        self.reader.close()
        self.reader = None

    async def test_read(self):

        self.assertTrue(await self.reader.connect())
        df = await self.reader.read()
        print(df)
        self.assertEqual(df.size, 40 * 2)


if __name__ == '__main__':
    unittest.main()
    