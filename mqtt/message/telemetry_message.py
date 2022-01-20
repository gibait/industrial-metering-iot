import logging
import time
import json


from kpn_senml import *


class TelemetryMessage(object):
    def __init__(self, **kwargs):
        """
        Telemetry message has all optional param because of
        build_class_from_senml_json method which needs an instance of self
        :param kwargs: type
        :param kwargs: value
        :param kwargs: unit
        :param kwargs: timestamp
        """
        self.type = kwargs.get('type', None)
        self.value = kwargs.get('value', None)
        self.unit = kwargs.get('unit', None) if kwargs.get('unit', None) else None
        self.timestamp = kwargs.get('timestamp', None) if kwargs.get('timestamp', None) else int(time.time())
        self.pack = SenmlPack("telemetry")

    def build_senml_json_payload(self):
        try:
            record = SenmlRecord(name=self.type,
                                 value=self.value,
                                 unit=self.unit
                                 )
            self.pack.add(record)
        except Exception as e:
            logging.error("Error Building Telemetry SenML Json payload")
            logging.error(e)
        finally:
            return self.pack.to_json()

    def build_class_from_senml_json(self, payload):
        """
        builds a Telemetry
        :param payload: pack.to_json()
        :return: self
        """
        try:
            # It does not load unit params, only name and value
            self.pack.from_json(payload)
            # Accessing kpn_SenmlRecord values
            self.type = self.pack._data[0].name
            self.value = self.pack._data[0].value
        except Exception as e:
            logging.error("Error crafting Telemetry Message from json")
            logging.error(e)
        finally:
            return self

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)