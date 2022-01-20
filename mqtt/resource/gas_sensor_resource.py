import asyncio
import random
import time
import uuid
import logging


from mqtt.resource.smart_object_resource import SmartObjectResource
from mqtt.conf.sensor_conf_values import GasConfValues as sensValues


class GasSensorResource(SmartObjectResource):

    RESOURCE_TYPE = sensValues.RESOURCE_TYPE

    def __init__(self):
        super().__init__(uuid.uuid4(), self.RESOURCE_TYPE)
        self.timestamp = int(time.time())
        self.gas_level = sensValues.MIN_USAGE + random.uniform(sensValues.MIN_USAGE, sensValues.MAX_USAGE) * random.uniform(0, 1)
        self.is_active = True

    async def update_gas_level(self):
        self.gas_level = self.gas_level + (sensValues.MIN_INCREASE + sensValues.MAX_INCREASE * random.uniform(0, 1))
        await self.notify_update(self.gas_level, type=self.type, unit=sensValues.UNIT)
        # TODO check if value < 0
        # TODO maybe add slow decrementing when not active

    async def start_periodic_event_value_update_task(self):
        try:
            logging.info(f"Starting periodic Update Task with period {sensValues.UPDATE_DELAY}s")
            await asyncio.sleep(sensValues.TASK_DELAY)
            while True:
                await asyncio.sleep(sensValues.UPDATE_DELAY)
                if self.is_active:
                    await self.update_gas_level()
                else:
                    logging.info(f"Gas supply monitored by device {self.id} is switched OFF")
        except Exception as e:
            logging.error("Error starting periodic event value update!")
            logging.error(e)

    def set_active(self, updated_value, **kwargs):
        self.is_active = updated_value