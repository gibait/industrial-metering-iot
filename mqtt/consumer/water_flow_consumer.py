import json
import uuid

import paho.mqtt.client as mqtt
from mqtt.conf.mqtt_conf_params import MqttConfigurationParams as mqttParams
from mqtt.message.telemetry_message import TelemetryMessage
from mqtt.resource.water_sensor_resource import WaterSensorResource


SUPPLY_THRESHOLD = 20.0


class WaterFlowConsumer:
    def __init__(self):
        self.id = str(uuid.uuid4())
        self.mqtt_client = mqtt.Client(self.id)
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.connect(mqttParams.BROKER_ADDRESS, mqttParams.BROKER_PORT)
        self.mqtt_client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        try:
            if str(rc) == 0:
                print("Connected successfully")

            device_telemetry_topic = "{0}/{1}/+/{2}".format(
                mqttParams.MQTT_DEFAULT_TOPIC,
                mqttParams.WATER_DEVICE_TOPIC,
                mqttParams.TELEMETRY_TOPIC
            )
            self.mqtt_client.subscribe(device_telemetry_topic)
            print("Subscribed to: " + device_telemetry_topic)
        except Exception as e:
            print("Error! Could not establish connection")
            print(e)

    def on_message(self, client, userdata, message):
        # message_payload = str(message.payload.decode("utf-8"))
        # print(f"Received {'retained' if message.retain else ''} IoT message: \n\tTopic: {message.topic} \n\tPayload: {message_payload}")

        telemetry_message:TelemetryMessage = self.parse_telemetry_message(message)
        if telemetry_message.type == WaterSensorResource.RESOURCE_TYPE:
            water_flow_level = telemetry_message.value
            print(f"New Water Flow Level received! Water flow level: {water_flow_level}")
            if self.is_water_level_alarm(water_flow_level) < 0:
                print("Water Supply Threshold reached! Activating actuator ...")
                # TODO add control message

    def is_water_level_alarm(self, water_level):
        return SUPPLY_THRESHOLD - water_level

    def parse_telemetry_message(self, mqtt_message:mqtt.MQTTMessage):
        try:
            if isinstance(mqtt_message, mqtt.MQTTMessage):
                payload = json.loads(mqtt_message.payload.decode("utf-8"))
                # Crafting Telemetry message for better parsing
                return TelemetryMessage(**payload)
        except Exception as e:
            print("Error! Cannot parse message")
            print(e)

def main():
    WaterFlowConsumer()

if __name__ == '__main__':
    main()