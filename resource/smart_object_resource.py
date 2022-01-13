import asyncio
import uuid

from resource_data_listener import Event

class SmartObjectResource:
    def __init__(self, object_id, object_type):
        self.id = object_id
        self.type = object_type
        self.resource_data_listener = Event()

    async def notify_update(self, updated_value):
        if self.resource_data_listener:
            self.resource_data_listener(updated_value)
        else:
            print("No one is listening ...")

    def add_data_listener(self, listener_to_add):
        if self.resource_data_listener is not None:
            self.resource_data_listener.append(listener_to_add)

    def remove_data_listener(self, listener_to_remove):
        if self.resource_data_listener is not None and listener_to_remove in self.resource_data_listener:
            self.resource_data_listener.remove(listener_to_remove)