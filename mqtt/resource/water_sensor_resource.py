import asyncio
import random
import time
import uuid
import logging


from mqtt.resource.smart_object_resource import SmartObjectResource


class WaterSensorResource(SmartObjectResource):

    RESOURCE_TYPE = "iot:sensor:water"
    MAX_WATER_FLOW = 50.0
    MIN_WATER_FLOW = 10.0
    MAX_INCREASE = 1.0
    MIN_INCREASE = 0.1
    TASK_DELAY = 2  # seconds
    UPDATE_DELAY = 5  # seconds

    def __init__(self):
        super().__init__(uuid.uuid4(), self.RESOURCE_TYPE)
        self.timestamp = int(time.time())
        self.water_level = self.MIN_WATER_FLOW + random.uniform(self.MIN_WATER_FLOW, self.MAX_WATER_FLOW) * random.uniform(0, 1)
        self.is_active = True

    async def update_water_level(self):
        self.water_level = self.water_level + (self.MIN_INCREASE + self.MAX_INCREASE * random.uniform(0, 1))
        await self.notify_update(self.type, self.water_level)
        # TODO check if value < 0
        # TODO maybe add slow decrementing when not active

    async def start_periodic_event_value_update_task(self):
        try:
            logging.info(f"Starting periodic Update Task with period {self.UPDATE_DELAY}s")
            time.sleep(self.TASK_DELAY)
            while True:
                await asyncio.sleep(self.UPDATE_DELAY)
                if self.is_active:
                    await self.update_water_level()
                else:
                    logging.info(f"Water supply monitored by device {self.id} is switched OFF")
        except Exception as e:
            logging.error("Error starting periodic event value update!")
            logging.error(e)

    def set_active(self, type, updated_value):
        self.is_active = updated_value