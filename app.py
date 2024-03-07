"""Main application."""

import os
import json
import asyncio
import logging

from base.gateway import GatewayManager, ModbusRTUGatewayManager
from base.sensor import SensorManager
from feed_scale import FeedScaleManager


async def main():
    logging.basicConfig(
        filename="IoT.log", 
        encoding="utf-8",
        level=logging.INFO, 
        format="%(asctime)s, %(levelname)s, %(filename)s, %(funcName)s, %(message)s", 
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    try:
        #Read config
        if not os.path.isfile("config/config.json"):
            logging.error("config.json doesn't exist.")
            print("config.json doesn't exist.")
            raise FileNotFoundError

        with open("config/config.json") as file:
            config = json.load(file)

        #Initialize managers
        print("Start initializing managers.")
        logging.info("Start initializing managers.")

        gateway_managers: list[GatewayManager] = []
        generate_list = config["gateway_managers"]
        for [name, config_path] in generate_list:
            match name:
                case "ModbusRTUGatewayManager":
                    manager = ModbusRTUGatewayManager()
                    await manager.initialize(config_path)
                case _:
                    logging.error(f"Gateway manager \"{name}\" isn't defined.")
                    print(f"Gateway manager \"{name}\" isn't defined.")
                    raise ValueError
            gateway_managers.append(manager)
                
        sensor_managers: list[SensorManager] = []
        generate_list = config["sensor_managers"]
        for [name, config_path] in generate_list:
            match name:
                case "FeedScaleManager":
                    manager = FeedScaleManager()
                    await manager.initialize(config_path)
                case _:
                    logging.error(f"Sensor manager \"{name}\" isn't defined.")
                    print(f"Sensor manager \"{name}\" isn't defined.")
                    raise ValueError
            sensor_managers.append(manager)
            
        logging.info("Successfully intialized managers.")
        print("Successfully intialized managers.")

        #Begin reading loop
        tasks = []
        for manager in sensor_managers:
            tasks.append(asyncio.create_task(manager.run()))
        await asyncio.gather(*tasks)

    except Exception:
        #Stop here to allow user to see what happens
        input("WAIT")

if __name__ == "__main__":
    asyncio.run(main())