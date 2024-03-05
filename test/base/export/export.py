import asyncio
import unittest

from pandas import DataFrame

from base.manage import Manager, Report
from base.export import DataExporter, DataGenerator


class TestExporter(DataExporter):

    def __init__(self) -> None:
        super().__init__()

    async def export(self, data: DataFrame) -> None:
        print("Begin to export.")
        await asyncio.sleep(2)
        print("Exist.")


class ErrorExporter(DataExporter):

    def __init__(self) -> None:
        super().__init__()

    async def export(self, data: DataFrame) -> None:
        print("Begin to export")
        raise TimeoutError("Time out")


class TestManager(Manager):

    def __init__(self) -> None:
        super().__init__()

    def handle(self, report: Report) -> None:
        if not isinstance(report.sign, DataGenerator):
            raise ValueError("Wrong worker.")
        if isinstance(report.content, BaseException):
            raise report.content
        
    async def initialize(self) -> None:
        return


class TestGenerator(DataGenerator):

    def __init__(self) -> None:
        super().__init__()

    async def work(self):

        df = DataFrame()
        await self.notify_exporters(df)


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.worker = TestGenerator()

    def tearDown(self):
        self.worker = None

    async def test_generator(self):

        self.worker.add_exporter(TestExporter())
        self.worker.add_exporter(TestExporter())
        await self.worker.work()
        self.worker.add_exporter(ErrorExporter())
        self.worker.set_manager(TestManager())
        with self.assertRaises(TimeoutError):
            await self.worker.work()


if __name__ == '__main__':
    unittest.main()