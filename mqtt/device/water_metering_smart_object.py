import asyncio

import json
import time

import paho.mqtt.client as mqtt

from mqtt.resource.water_sensor_resource import WaterSensorResource
from mqtt.resource.generic_actuator_resource import GenericActuatorResource
from mqtt.message.telemetry_message import TelemetryMessage
from mqtt.message.control_message import ControlMessage
from mqtt.conf.mqtt_conf_params import MqttConfigurationParams as mqttParams

BASIC_TOPIC  = "metering/water/device"
TELEMETRY_TOPIC = "telemetry"
CONTROL_TOPIC = "control"

RESTART_DELAY = 10 #s

class WaterMeteringSmartObject:

    def __init__(self, device_id, mqtt_client:mqtt.Client, resources_dict):
        self.device_id = device_id
        self.mqtt_client = mqtt_client
        self.resources_dict = resources_dict
        print("Water Metering Smart Object successfully created !")
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.connect_async(
            host=mqttParams.BROKER_ADDRESS,
            port=mqttParams.BROKER_PORT,
            keepalive=10)
        self.mqtt_client.loop_start()
        self.loop = asyncio.get_event_loop()

    def on_connect(self, client, userdata, flags, rc):
        try:
            if str(rc) == 0:
                print("Connected successfully")
            device_telemetry_topic = "{0}/{1}/{2}/{3}".format(
                mqttParams.MQTT_DEFAULT_TOPIC,
                mqttParams.WATER_DEVICE_TOPIC,
                self.device_id,
                mqttParams.CONTROL_TOPIC
            )
            self.mqtt_client.subscribe(device_telemetry_topic)
            print("Subscribed to: " + device_telemetry_topic)
        except Exception as e:
            print("Error! Could not establish connection")
            print(e)

    def on_message(self, client, userdata, message):
        # TODO add try and catch
        try:
            control_message:ControlMessage = self.parse_control_message(message)

            if control_message.type == 'alert':

                print("CONTROL MESSAGE RECEIVED! Switching actuator ...")

                message_callback_loop = asyncio.new_event_loop()
                task = message_callback_loop.create_task(self.resources_dict['actuator'].switch_actuator_status())
                message_callback_loop.run_until_complete(task)

        except Exception as e:
            print("Error! Cannot properly compute message")
            print(e)

    def parse_control_message(self, mqtt_message: mqtt.MQTTMessage):
        try:
            if isinstance(mqtt_message, mqtt.MQTTMessage):
                payload = json.loads(mqtt_message.payload.decode("utf-8"))
                # Crafting Control message for better parsing
                return ControlMessage(**payload)
        except Exception as e:
            print("Error! Cannot parse message")
            print(e)

    def start(self):
        try:
            if self.device_id and len(self.resources_dict.keys()) > 0:
                print("Starting Water Metering Emulator ...")
                self.loop.create_task(self.register_to_available_resources())
                self.loop.run_forever()
        except Exception as e:
            print("Error starting Water Metering Emulator! ")
            print(e)

    def stop(self):
        # TODO add coherent stop method
        """
        - deregister to data listener
        - cancel task
        - stop loop
        """

    async def register_to_available_resources(self):
        try:
            for resource in self.resources_dict:
                # TODO add actuator status check before starting periodic update
                if isinstance(self.resources_dict[resource], WaterSensorResource) or isinstance(self.resources_dict[resource], GenericActuatorResource):

                    print(f"Registering to Resource {self.resources_dict[resource].type} (id: {self.resources_dict[resource].id}) notifications")
                    # Registering to data listener for both actuator and sensor
                    self.resources_dict[resource].add_data_listener(self.on_data_changed)
                    # Linking actuator status to sensor
                    if self.resources_dict['actuator']:
                        self.resources_dict['actuator'].add_data_listener(self.resources_dict['sensor'].set_active)

                    if self.resources_dict[resource].type == 'iot:sensor:waterflow':
                        # Starting periodic event value update for emulating purposes
                        self.loop.create_task(self.resources_dict["sensor"].start_periodic_event_value_update_task(), name="periodic_event_value_update")

        except Exception as e:
            print("Error Registering to resources! ")
            print(e)


    def on_data_changed(self, type, updated_value):
        try:
            message = TelemetryMessage(type, updated_value)
            self.publish_telemetry_data(
                topic=f"{BASIC_TOPIC}/{self.device_id}/{TELEMETRY_TOPIC}",
                message=message
            )
        except Exception as e:
            print("Error! No data received")
            print(e)

    def publish_telemetry_data(self, topic, message):
        # TODO add is connected check
        if topic and message:
            try:
                self.mqtt_client.publish(topic, message.to_json())
                print(f"Data {message.to_json()} successfully published to {topic}")
            except Exception as e:
                print("Error! Could not publish data")
                print(e)
        else:
            print("Error! msg != None or topic != None or mqttClient not connected")
