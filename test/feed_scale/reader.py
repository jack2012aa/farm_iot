import asyncio
import unittest

from feed_scale.reader import FeedScaleRTUReader


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.reader = FeedScaleRTUReader(length=40, duration=0.2, slave=2)
        pass

    def tearDown(self):
        self.reader = None
        pass

    async def test_read(self):

        await self.reader.connect(port="com2")
        df = await self.reader.read()
        print(df)


if __name__ == '__main__':
    unittest.main()