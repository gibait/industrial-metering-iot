import logging
import time
import json


from kpn_senml import *


class TelemetryMessage(object):

    def __init__(self, **kwargs):
        """
        Telemetry message has all optional param because of
        build_class_from_senml_json method which needs an instance of self
        :param kwargs: name (UUID)
        :param kwargs: type iot:(sensor)(actuator):[resource]
        :param kwargs: value
        :param kwargs: unit
        :param kwargs: timestamp
        """
        self.name = kwargs.get('name', None)
        self.type = kwargs.get('type', None)
        self.value = kwargs.get('value', None)
        self.unit = kwargs.get('unit', None) if kwargs.get('unit', None) else None
        self.timestamp = kwargs.get('timestamp', None) if kwargs.get('timestamp', None) else int(time.time())

    def build_senml_json_payload(self):
        pack = SenmlPack(self.type)
        try:
            record = SenmlRecord(name=str(self.name),
                                 value=self.value,
                                 unit=self.unit,
                                 time=self.timestamp
                                 )
            pack.add(record)
        except Exception as e:
            logging.error("Error Building Telemetry SenML Json payload")
            logging.error(e)
        finally:
            return pack.to_json()

    def build_class_from_senml_json(self, payload):
        """
        builds a Telemetry
        :param payload: pack.to_json()
        :return: self
        """
        try:
            records = json.loads(payload)
            for record in records:
                self.type = record["bn"]
                self.value = record["v"] if "v" in record else record['vb']
                self.unit = record["u"] if "u" in record else None
                self.name = record["n"]
        except Exception as e:
            logging.error("Error crafting Telemetry Message from json")
            logging.error(e)
        finally:
            return self

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__).replace("name", "n").replace("value", "v").replace("timestamp", "t").replace("unit", "u").replace("type", "bn")