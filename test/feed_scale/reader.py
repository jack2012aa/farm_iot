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

        if await self.reader.connect(port="COM2"):
            print(await self.reader.read())
        else:
            raise ConnectionError("Fail to connect to gateway.")

if __name__ == '__main__':
    unittest.main()
    