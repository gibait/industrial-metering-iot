import json

class DeviceInfoMessage(object):

    def __init__(self, id, software_version, manufacturer, location, plant):
        """
        Telemetry message has all optional param because of
        build_class_from_senml_json method which needs an instance of self
        :param id (UUID)
        :param software_version
        :param manufacturer
        :param location
        :param plant
        """
        self.id = str(id)
        self.software_version = software_version
        self.manufacturer = manufacturer
        self.location = location
        self.plant = plant

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)