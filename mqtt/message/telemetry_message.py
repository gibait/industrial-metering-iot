import time

import json


class TelemetryMessage(object):
    def __init__(self, type, value, **kwargs):
        self.type = type
        self.value = value
        self.timestamp = kwargs.get('timestamp', None) if kwargs.get('timestamp', None) else int(time.time())

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)