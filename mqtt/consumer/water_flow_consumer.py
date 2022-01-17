import json
import time
import uuid
from threading import Timer

import paho.mqtt.client as mqtt
from mqtt.conf.mqtt_conf_params import MqttConfigurationParams as mqttParams
from mqtt.message.telemetry_message import TelemetryMessage
from mqtt.message.control_message import ControlMessage
from mqtt.resource.water_sensor_resource import WaterSensorResource
from mqtt.resource.generic_actuator_resource import GenericActuatorResource


SUPPLY_THRESHOLD = 2.0
RESTART_DELAY = 10 #s

class WaterFlowConsumer:

    is_alarm_notified = False

    def __init__(self):
        self.id = str(uuid.uuid4())
        self.waterflow_history = {}
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
        try:
            telemetry_message:TelemetryMessage = self.parse_telemetry_message(message)
            if telemetry_message.type == WaterSensorResource.RESOURCE_TYPE:
                water_flow_level = telemetry_message.value
                print(f"New Water Flow Level received! Water flow level: {water_flow_level}")

                if message.topic not in self.waterflow_history:
                    print(f"New Water flow level saved for {message.topic}")
                    self.waterflow_history[message.topic] = water_flow_level
                    self.is_alarm_notified = False
                else:
                    if self.is_water_level_alarm(self.waterflow_history.get(message.topic), water_flow_level) and not self.is_alarm_notified:
                        print("WATER SUPPLY THRESHOLD LEVEL REACHED! Sending control notification ...")
                        self.is_alarm_notified = True

                        # topic string manipulation by adding control to it
                        topic = message.topic.split('/')[:-1]
                        topic.append(mqttParams.CONTROL_TOPIC)
                        control_topic = '/'.join(topic)

                        self.publish_control_message(control_topic, ControlMessage('alert', {WaterSensorResource.RESOURCE_TYPE:"threshold_reached"}))
                        # resetting waterflow_history
                        self.waterflow_history = {}

                        # TODO sleep and resend control message
                        t = Timer(RESTART_DELAY, self.delay_control_message, [control_topic])
                        t.start()

            if telemetry_message.type == GenericActuatorResource.RESOURCE_TYPE:
                actuator_status = telemetry_message.value
                print(f"Supply switched {'on' if actuator_status else 'off'}")

        except Exception as e:
            print("Error! Could not create message")
            print(e)

    def delay_control_message(self, topic):
        self.publish_control_message(topic, ControlMessage('alert', {WaterSensorResource.RESOURCE_TYPE: "resume_operation"}))

    def is_delay_over(self, suspend_starting_time):
        return int(time.time()) - suspend_starting_time >= RESTART_DELAY

    def is_water_level_alarm(self, last_water_level, new_water_level):
        return new_water_level - last_water_level >= SUPPLY_THRESHOLD

    def parse_telemetry_message(self, mqtt_message:mqtt.MQTTMessage):
        try:
            if isinstance(mqtt_message, mqtt.MQTTMessage):
                payload = json.loads(mqtt_message.payload.decode("utf-8"))
                # Crafting Telemetry message for better parsing
                return TelemetryMessage(**payload)
        except Exception as e:
            print("Error! Cannot parse message")
            print(e)

    def publish_control_message(self, topic:str, message:ControlMessage):
        # TODO add is connected check
        if topic and message:
            try:
                self.mqtt_client.publish(topic, message.to_json())
                print(f"Data {message.to_json()} successfully published to {topic}")
            except Exception as e:
                print("Error! Could not publish message")
                print(e)
        else:
            print("Error! msg != None or topic != None or mqttClient not connected")

def main():
    WaterFlowConsumer()

if __name__ == '__main__':
    main()