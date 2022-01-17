import asyncio
import random
import time
import uuid


from mqtt.resource.smart_object_resource import SmartObjectResource


MAX_WATER_FLOW = 50.0
MIN_WATER_FLOW = 10.0
MAX_INCREASE = 1.0
MIN_INCREASE = 0.1
TASK_DELAY = 2 #seconds
UPDATE_DELAY = 3 #seconds

class WaterSensorResource(SmartObjectResource):

    RESOURCE_TYPE = "iot:sensor:waterflow"

    def __init__(self):
        super().__init__(uuid.uuid4(), self.RESOURCE_TYPE)
        self.timestamp = int(time.time())
        self.water_level = MIN_WATER_FLOW + random.uniform(MIN_WATER_FLOW, MAX_WATER_FLOW) * random.uniform(0, 1)
        self.is_active = True
        # self.start_periodic_event_value_update_task()

    async def update_water_level(self):
        self.water_level = self.water_level + (MIN_INCREASE + MAX_INCREASE * random.uniform(0, 1))
        await self.notify_update(self.type, self.water_level)
        # TODO check if value < 0
        # TODO maybe add slow decrementing when not active

    async def start_periodic_event_value_update_task(self):
        try:
            print(f"Starting periodic Update Task with period {UPDATE_DELAY}s")
            # self.add_data_listener(on_data_changed)
            time.sleep(TASK_DELAY)
            while True:
                await asyncio.sleep(UPDATE_DELAY)
                if self.is_active:
                    await self.update_water_level()
                else:
                    print("Supply is switched OFF")
        except Exception as e:
            print("Error starting periodic event value update!")
            print(e)

    def set_active(self, type, updated_value):
        self.is_active = updated_value

def on_data_changed(water_level):
    print(f"New Water Level: {water_level} l/s")

# def main():
#     w = WaterSensorResource()
#
# if __name__ == '__main__':
#     main()