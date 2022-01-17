import json
import time


class GenericMessage(object):
    def __init__(self, type, metadata, **kwargs):
        self.type = type
        self.metadata = metadata
        self.timestamp = kwargs.get('timestamp', None) if kwargs.get('timestamp', None) else int(time.time())

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)