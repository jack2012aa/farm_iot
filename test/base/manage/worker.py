import json
import unittest
import asyncio

from base.manage import Manager, Report, Worker


class TestManager(Manager):

    def __init__(self) -> None:
        super().__init__()

    def handle(self, report: Report) -> None:
        # print("I found an error.", report.sign, report.content)
        if isinstance(report.content, BaseException):
            raise report.content
        
    async def initialize(self) -> None:
        return
    

class TestWorker(Worker):

    def  __init__(self) -> None:
        super().__init__()

    async def make_an_error(self):
        print("Make an error")
        raise ValueError("I make an error!")
    
    async def sleep(self):
        print("Sleep")
        await asyncio.sleep(2)
        print("Wake up")

    async def work(self):
        tasks = []
        tasks.append(self.sleep())
        tasks.append(self.make_an_error())
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                await self.notify_manager(Report(sign=self, content=result))


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.manager = TestManager()
        self.worker = TestWorker()

    def tearDown(self):
        pass

    async def test_worker(self):

        self.worker.set_manager(self.manager)
        with self.assertRaises(ValueError):
            await self.worker.work()

    async def test_send_email(self):

        with open("test/base/manage/email_settings.json") as file:
            email_settings = json.load(file)
        self.manager.email_settings = email_settings
        await self.manager.send_alarm_email(tuple(["Chang-Yu Huang"]), "Testing.")
        await self.manager.send_alarm_email(tuple(["Chang-Yu Huang", "Fake"]), "Testing.")


if __name__ == '__main__':
    unittest.main()