import asyncio
import random
import time
import uuid

import json

from mqtt.resource.smart_object_resource import SmartObjectResource

RESOURCE_TYPE = "iot:sensor:waterflow"

MAX_WATER_FLOW = 50.0
MIN_WATER_FLOW = 10.0
MAX_INCREASE = 1.0
MIN_INCREASE = 0.1
TASK_DELAY = 2 #seconds
UPDATE_DELAY = 3 #seconds

class WaterSensorResource(SmartObjectResource):

    def __init__(self):
        super().__init__(uuid.uuid4(), RESOURCE_TYPE)
        self.timestamp = int(time.time())
        self.water_level = MIN_WATER_FLOW + random.uniform(MIN_WATER_FLOW, MAX_WATER_FLOW) * random.uniform(0, 1)
        # self.start_periodic_event_value_update_task()

    async def update_water_level(self):
        self.water_level = self.water_level - (MIN_INCREASE + MAX_INCREASE * random.uniform(0, 1))
        await self.notify_update(self.type, self.water_level)
        await asyncio.sleep(TASK_DELAY)
        # TODO check if value < 0

    def start_periodic_event_value_update_task(self):
        try:
            print(f"Starting periodic Update Task with period {UPDATE_DELAY}s")
            # self.add_data_listener(print_water_level)
            time.sleep(TASK_DELAY)
            while True:
                task = asyncio.get_event_loop().create_task(self.update_water_level())
                asyncio.get_event_loop().run_until_complete(task)
        except Exception as e:
            print("Error starting periodic event value update!")
            print(e)

def print_water_level(water_level):
    print(f"New Water Level: {water_level} l/s")

# def main():
#     w = WaterSensorResource()
#
# if __name__ == '__main__':
#     main()