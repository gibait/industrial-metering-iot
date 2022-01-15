import asyncio

import paho.mqtt.client as mqtt

from mqtt.resource.water_sensor_resource import WaterSensorResource
from mqtt.resource.generic_actuator_resource import GenericActuatorResource
from mqtt.message.telemetry_message import TelemetryMessage

BASIC_TOPIC  = "metering/water/device"
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
                task = asyncio.get_event_loop().create_task(self.register_to_available_resources())
                asyncio.get_event_loop().run_forever()
        except Exception as e:
            print("Error starting Water Metering Emulator! ")
            print(e)

    def stop(self):
        # TODO add coherent stop method
        pass

    async def register_to_available_resources(self):
        try:
            for resource in self.resources_dict:
                if isinstance(self.resources_dict[resource], WaterSensorResource) or isinstance(self.resources_dict[resource], GenericActuatorResource):
                    print(f"Registering to Resource {self.resources_dict[resource].type} (id: {self.resources_dict[resource].id}) notifications")
                    # Registering to data listener for both actuator and sensor
                    self.resources_dict[resource].add_data_listener(self.on_data_changed)

                    if self.resources_dict[resource].type == 'iot:sensor:waterflow':
                        # Starting periodic event value update for emulating purposes
                        asyncio.get_running_loop().create_task(self.resources_dict["sensor"].start_periodic_event_value_update_task())

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
