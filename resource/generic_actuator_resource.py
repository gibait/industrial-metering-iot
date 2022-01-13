import uuid

from smart_object_resource import SmartObjectResource

RESOURCE_TYPE = "iot:actuator:generic"


TASK_DELAY = 2 #seconds
UPDATE_DELAY = 3 #seconds


class GenericActuatorResource(SmartObjectResource):
    def __init__(self):
        super().__init__(uuid.uuid4(), RESOURCE_TYPE)
        self.value = False

    def switch_actuator_status(self):
        self.value = not self.value
        self.notify_update(self.value)