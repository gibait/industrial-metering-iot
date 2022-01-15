import uuid

from mqtt.resource.smart_object_resource import SmartObjectResource

RESOURCE_TYPE = "iot:actuator:generic"


TASK_DELAY = 2 #seconds
UPDATE_DELAY = 3 #seconds


class GenericActuatorResource(SmartObjectResource):
    def __init__(self):
        super().__init__(uuid.uuid4(), RESOURCE_TYPE)
        self.value = False

    async def switch_actuator_status(self):
        self.value = not self.value
        await self.notify_update(self.type, self.value)