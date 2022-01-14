import json
import time


class GenericMessage:
    def __init__(self, type, metadata):
        self.timestamp = int(time.time())
        self.type = type
        self.metadata = metadata

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)