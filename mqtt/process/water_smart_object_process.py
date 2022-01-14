import uuid

import paho.mqtt.client as mqtt

from mqtt.device.water_metering_smart_object import WaterMeteringSmartObject
from mqtt.conf.mqtt_conf_params import MqttConfigurationParams as mqttParams
from mqtt.resource.water_sensor_resource import WaterSensorResource
from mqtt.resource.generic_actuator_resource import GenericActuatorResource

class WaterSmartObjectProcess:
    def __init__(self):
        self.device_id = uuid.uuid4()
        self.mqtt_client = mqtt.Client(str(self.device_id), clean_session=True)
        self.mqtt_client.connect(
            host=mqttParams.BROKER_ADDRESS,
            port=mqttParams.BROKER_PORT,
            keepalive=10)
        try:
            water_metering_smart_object = WaterMeteringSmartObject(self.device_id, self.mqtt_client, {
                "sensor" : WaterSensorResource(),
                "actuator" : GenericActuatorResource()
            })

            water_metering_smart_object.start()
        except Exception as e:
            print(e)

def main():
    WaterSmartObjectProcess()

if __name__ == '__main__':
    main()