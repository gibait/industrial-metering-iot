import uuid

from mqtt.resource.smart_object_resource import SmartObjectResource
from mqtt.conf.sensor_conf_values import GenericActuator as sensValues


class GenericActuatorResource(SmartObjectResource):

    RESOURCE_TYPE = sensValues.RESOURCE_TYPE

    def __init__(self):
        super().__init__(uuid.uuid4(), self.RESOURCE_TYPE)
        self.value = True

    async def switch_actuator_status(self):
        self.value = not self.value
        await self.notify_update(self.value, type=self.type, unit=None)