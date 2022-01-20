import logging

from mqtt.resource.resource_data_listener import Event

class SmartObjectResource:
    def __init__(self, object_id, object_type):
        self.id = object_id
        self.type = object_type
        self.resource_data_listener = Event()

    async def notify_update(self, updated_value, **kwargs):
        if self.resource_data_listener:
            self.resource_data_listener(updated_value, **kwargs)
        else:
            logging.info("No one is listening ...")

    def add_data_listener(self, listener_to_add):
        self.resource_data_listener.append(listener_to_add)

    def remove_data_listener(self, listener_to_remove):
        if self.resource_data_listener and listener_to_remove in self.resource_data_listener:
            self.resource_data_listener.remove(listener_to_remove)