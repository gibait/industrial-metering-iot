import time

import json


class TelemetryMessage:
    def __init__(self, message_type, value):
        self.timestamp = int(time.time())
        self.type = message_type
        self.value = value

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)