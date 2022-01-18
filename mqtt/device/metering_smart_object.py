import asyncio
import logging
import json
import uuid

import paho.mqtt.client as mqtt
from asyncio_mqtt import Client

from mqtt.resource.gas_sensor_resource import GasSensorResource
from mqtt.resource.water_sensor_resource import WaterSensorResource
from mqtt.resource.generic_actuator_resource import GenericActuatorResource
from mqtt.message.telemetry_message import TelemetryMessage
from mqtt.message.control_message import ControlMessage
from mqtt.conf.mqtt_conf_params import MqttConfigurationParams as mqttParams


logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)-8s %(message)s")

class MeteringSmartObject:

    def __init__(self, resources_dict, type):
        self.device_id = uuid.uuid4()
        # Using async-mqtt wrapper for complete async compatibility
        self.mqtt_client = Client(
            hostname=mqttParams.BROKER_ADDRESS,
            port=mqttParams.BROKER_PORT,
            clean_session=True,
            client_id=str(self.device_id),
            keepalive=10
            )
        self.resources_dict = resources_dict
        self.type = type
        logging.info(f"{self.type} Metering Smart Object successfully created !")

    async def subscribe_to_control_topic(self):
        try:
            device_telemetry_topic = "{0}/{1}/{2}/{3}/{4}".format(
                mqttParams.MQTT_DEFAULT_TOPIC,
                self.type,
                mqttParams.DEVICE_TOPIC,
                self.device_id,
                mqttParams.CONTROL_TOPIC
            )
            await self.mqtt_client.subscribe(device_telemetry_topic)
            logging.info("Subscribed to: " + device_telemetry_topic)
        except Exception as e:
            logging.error("Error subscribing to control topic!")
            logging.error(e)

    async def on_connect(self):
        try:
            await self.mqtt_client.connect()
            logging.info(f"Device {self.device_id} successfully connected to MQTT Broker")
        except Exception as e:
            logging.error("Error! Could not establish connection")
            logging.error(e)

    async def on_message(self):
        try:
            async with self.mqtt_client.filtered_messages('#') as messages:
                async for message in messages:
                    control_message:ControlMessage = self.parse_control_message(message)
                    if control_message.type == 'alert':
                        logging.info(f"CONTROL MESSAGE RECEIVED! Switching {self.type} actuator ...")
                        await self.resources_dict['actuator'].switch_actuator_status()
        except Exception as e:
            logging.error("Error processing incoming message!")
            logging.error(e)

    def parse_control_message(self, mqtt_message: mqtt.MQTTMessage):
        try:
            if isinstance(mqtt_message, mqtt.MQTTMessage):
                payload = json.loads(mqtt_message.payload.decode("utf-8"))
                # Crafting Control message for better parsing
                return ControlMessage(**payload)
        except Exception as e:
            logging.error("Error! Cannot parse message")
            logging.error(e)

    async def start(self):
        try:
            if self.device_id and len(self.resources_dict.keys()) > 0:
                logging.info(f"Starting {self.type} Metering Emulator ...")

                await self.on_connect()
                # Creating background task to receive messages
                asyncio.get_event_loop().create_task(self.on_message())
                await self.subscribe_to_control_topic()
                await self.register_to_available_resources()

        except Exception as e:
            logging.error(f"Error starting {self.type} Metering Emulator! ")
            logging.error(e)

    def stop(self):
        # TODO add coherent stop method
        '''
        - deregister to data listener
        - cancel task
        - stop loop
        '''

    async def register_to_available_resources(self):
        try:
            for resource in self.resources_dict:
                # TODO add actuator status check before starting periodic update
                if isinstance(self.resources_dict[resource], GasSensorResource) or isinstance(self.resources_dict[resource], WaterSensorResource) or isinstance(self.resources_dict[resource], GenericActuatorResource):
                    logging.info(f"Registering to Resource {self.resources_dict[resource].type} (id: {self.resources_dict[resource].id}) notifications")
                    # Registering to data listener for both actuator and sensor
                    self.resources_dict[resource].add_data_listener(self.on_data_changed)
                    # Linking actuator status to sensor
                    if self.resources_dict['actuator']:
                        self.resources_dict['actuator'].add_data_listener(self.resources_dict['sensor'].set_active)

                    # Starting periodic event value update for emulating purposes
                    if resource == "sensor":
                        asyncio.get_event_loop().create_task(self.resources_dict[resource].start_periodic_event_value_update_task())

        except Exception as e:
            logging.error("Error Registering to resources! ")
            logging.error(e)

    def on_data_changed(self, type, updated_value):
        try:
            message = TelemetryMessage(type, updated_value)
            asyncio.get_event_loop().create_task(self.publish_telemetry_data(
                topic=f"{mqttParams.MQTT_DEFAULT_TOPIC}/{self.type}/{mqttParams.DEVICE_TOPIC}/{self.device_id}/{mqttParams.TELEMETRY_TOPIC}",
                message=message
            ))
        except Exception as e:
            logging.error("Error! No data received")
            logging.error(e)

    async def publish_telemetry_data(self, topic, message):
        # TODO add is connected check
        if topic and message:
            try:
                await self.mqtt_client.publish(topic, message.to_json())
                logging.info(f"Data {message.to_json()} successfully published to {topic}")
            except Exception as e:
                logging.error("Error! Could not publish data")
                logging.error(e)
        else:
            logging.error("Error! msg != None or topic != None or mqttClient not connected")
