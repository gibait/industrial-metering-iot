import asyncio
import random
import time
import uuid
import logging


from mqtt.resource.smart_object_resource import SmartObjectResource


class GasSensorResource(SmartObjectResource):

    RESOURCE_TYPE = "iot:sensor:gas"
    MAX_GAS_USAGE = 10.0
    MIN_GAS_USAGE = 1.0
    MAX_INCREASE = 0.5
    MIN_INCREASE = 0.1
    TASK_DELAY = 2  # seconds
    UPDATE_DELAY = 3  # seconds

    def __init__(self):
        super().__init__(uuid.uuid4(), self.RESOURCE_TYPE)
        self.timestamp = int(time.time())
        self.gas_level = self.MIN_GAS_USAGE + random.uniform(self.MIN_GAS_USAGE, self.MAX_GAS_USAGE) * random.uniform(0, 1)
        self.is_active = True
        # self.start_periodic_event_value_update_task()

    async def update_gas_level(self):
        self.gas_level = self.gas_level + (self.MIN_INCREASE + self.MAX_INCREASE * random.uniform(0, 1))
        await self.notify_update(self.type, self.gas_level)
        # TODO check if value < 0
        # TODO maybe add slow decrementing when not active

    async def start_periodic_event_value_update_task(self):
        try:
            logging.info(f"Starting periodic Update Task with period {self.UPDATE_DELAY}s")
            # self.add_data_listener(on_data_changed)
            time.sleep(self.TASK_DELAY)
            while True:
                await asyncio.sleep(self.UPDATE_DELAY)
                if self.is_active:
                    await self.update_gas_level()
                else:
                    logging.info(f"Gas supply monitored by device {self.id} is switched OFF")
        except Exception as e:
            logging.error("Error starting periodic event value update!")
            logging.error(e)

    def set_active(self, type, updated_value):
        self.is_active = updated_value