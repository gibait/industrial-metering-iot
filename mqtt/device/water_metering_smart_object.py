import asyncio

import paho.mqtt.client as mqtt

from mqtt.resource.water_sensor_resource import WaterSensorResource
from mqtt.resource.generic_actuator_resource import GenericActuatorResource
from mqtt.message.telemetry_message import TelemetryMessage

BASIC_TOPIC  = "metering/water"
TELEMETRY_TOPIC = "telemetry"
COMMAND_TOPIC = "command"

class WaterMeteringSmartObject:
    def __init__(self, device_id, mqtt_client, resources_dict):
        self.device_id = device_id
        self.mqtt_client = mqtt_client
        self.resources_dict = resources_dict
        print("Water Metering Smart Object successfully created !")

    def start(self):
        try:
            if self.device_id and len(self.resources_dict.keys()) > 0:
                print("Starting Water Metering Emulator ...")
                self.register_to_available_resources()
        except Exception as e:
            print("Error starting Water Metering Emulator! ")
            print(e)

    def stop(self):
        # TODO add coherent stop method
        pass

    def register_to_available_resources(self):
        try:
            for resource in self.resources_dict:
                if isinstance(self.resources_dict[resource], WaterSensorResource) or isinstance(self.resources_dict[resource], GenericActuatorResource):

                    print(f"Registering to Resource {self.resources_dict[resource].type} (id: {self.resources_dict[resource].id}) notifications")

                    self.resources_dict[resource].add_data_listener(self.on_data_changed)
                    if resource == 'sensor':
                        self.resources_dict["sensor"].start_periodic_event_value_update_task()

        except Exception as e:
            print("Error Registering to resources! ")
            print(e)

    def on_data_changed(self, type, updated_value):
        try:
            message = TelemetryMessage(type, updated_value)
            self.publish_telemetry_data(
                topic=f"{BASIC_TOPIC}/{TELEMETRY_TOPIC}",
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
