import asyncio
import unittest

import paho.mqtt.client as mqtt

from base.gateway.mqtt import MQTTClientManager
from auto_feeder_gate_manager import *


class MyTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.client = MQTTClientManager()
        self.manager = AutoFeederGateManager()
    
    def tearDown(self):
        self.manager = None
        self.client.disconnect()
    
    async def test_initialize(self):
        
        self.client.initialize("test/auto_feeder_gate_manager/settings/mqtt_settings.json")
        with self.assertRaises(KeyError):
            await self.manager.initialize("test/auto_feeder_gate_manager/settings/wrong_topic.json")
        with self.assertRaises(FileNotFoundError):
            await self.manager.initialize("fake_path")
        await self.manager.initialize("test/auto_feeder_gate_manager/settings/gate_topic.json")
        
    async def get_status(self):
        self.client.initialize("test/auto_feeder_gate_manager/settings/mqtt_settings.json")
        await self.manager.initialize("test/auto_feeder_gate_manager/settings/gate_topic.json")
        publisher = 
    

if __name__ == '__main__':
    unittest.main()