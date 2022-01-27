import asyncio
import logging
import json
import uuid

import paho.mqtt.client as mqtt
from asyncio_mqtt import Client

from mqtt.resource.gas_sensor_resource import GasSensorResource
from mqtt.resource.water_sensor_resource import WaterSensorResource
from mqtt.resource.electricity_sensor_resource import ElectricitySensorResource
from mqtt.resource.generic_actuator_resource import GenericActuatorResource
from mqtt.message.telemetry_message import TelemetryMessage
from mqtt.message.control_message import ControlMessage
from mqtt.conf.mqtt_conf_params import MqttConfigurationParams as mqttParams


logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)-8s %(message)s")

DEMO_RUNTIME = 10

class MeteringSmartObject:
    """
    Core of smart object emulation
    Handle all three resources type
    Subscribes to devices control topic
    When receives an alert it switches actuator's device status
    When data listener send a notify_update it publishes a telemetry message
    Is started by an async start function
    Is stopped after DEMO_RUNTIME seconds
    """

    def __init__(self, resources_dict, type, location, plant):
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
        self.location = location
        self.plant = plant
        logging.info(f"{self.type} Metering Smart Object successfully created !")

    async def subscribe_to_control_topic(self):
        """
        Subscribe to every device's control topic available
        /metering/[location]/[plant]/[resource]/device/[unique_id]/(control)
        """
        try:
            device_telemetry_topic = "{0}/{1}/{2}/{3}/{4}/{5}/{6}".format(
                mqttParams.MQTT_DEFAULT_TOPIC,
                self.location,
                self.plant,
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
        """
        Connect to mqtt broker
        """
        try:
            await self.mqtt_client.connect()
            logging.info(f"Device {self.device_id} successfully connected to MQTT Broker")
        except Exception as e:
            logging.error("Error! Could not establish connection")
            logging.error(e)

    async def on_message(self):
        """
        Iterative async function
        When a message is received it parses it
        only checks if type == 'alert'
        discards metadata
        """
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
        """
        Build ControlMessage from json payload
        :param mqtt_message:
        :return: ControlMessage
        """
        try:
            if isinstance(mqtt_message, mqtt.MQTTMessage):
                payload = json.loads(mqtt_message.payload.decode("utf-8"))
                # Crafting Control message for better parsing
                return ControlMessage(**payload)
        except Exception as e:
            logging.error("Error! Cannot parse message")
            logging.error(e)

    async def start(self):
        """
        Starts smart object emulation
        :return:
        """
        try:
            if self.device_id and len(self.resources_dict.keys()) > 0:
                logging.info(f"Starting {self.type} Metering Emulator ...")
                logging.info(f"Running simulation for {DEMO_RUNTIME} seconds")

                await self.on_connect()
                # Creating background task to receive messages
                asyncio.get_event_loop().create_task(self.on_message(), name=str(self.device_id)+"on_message")
                await self.subscribe_to_control_topic()
                await self.register_to_available_resources()
                # asyncio.get_event_loop().create_task(self.stop())

        except Exception as e:
            logging.error(f"Error starting {self.type} Metering Emulator! ")
            logging.error(e)

    async def stop(self):
        """
        Sleeps for simulation time and then stops tasks and loop
        When done starts calls write to file function
        :return:
        """
        try:
            await asyncio.sleep(DEMO_RUNTIME)
            await self.deregister_to_resources()
            for task in asyncio.all_tasks():
                if task.get_name() == str(self.device_id)+"on_message" or task.get_name() == str(self.device_id)+"periodic":
                    task.cancel()
            logging.info("Stopping ...")
        except Exception as e:
            logging.error("Error interrupting task")
            logging.error(e)

    async def deregister_to_resources(self):
        """
        De-register from resources
        :return:
        """
        try:
            for resource in self.resources_dict:
                self.resources_dict[resource].remove_data_listener(resource)
                logging.info(f"Removed {resource} data listener for resource {self.device_id}")
        except Exception as e:
            logging.error("Error de-registering to resources! ")
            logging.error(e)

    async def register_to_available_resources(self):
        """
        Most important piece of the puzzle
        - It checks if resources given are allowed
        - It registers to resources data listener
        - It registers sensor to actuator data listener
        - It creates periodic event value update task
        :return:
        """
        try:
            for resource in self.resources_dict:
                if isinstance(self.resources_dict[resource], (GasSensorResource, WaterSensorResource, ElectricitySensorResource, GenericActuatorResource)):
                    logging.info(f"Registering to Resource {self.resources_dict[resource].type} (id: {self.resources_dict[resource].id}) notifications")
                    # Registering to data listener for both actuator and sensor
                    self.resources_dict[resource].add_data_listener(self.on_data_changed)
                    # Linking actuator status to sensor
                    if self.resources_dict['actuator']:
                        self.resources_dict['actuator'].add_data_listener(self.resources_dict['sensor'].set_active)

                    # Starting periodic event value update for emulating purposes
                    if resource == "sensor":
                        asyncio.get_event_loop().create_task(self.resources_dict[resource].start_periodic_event_value_update_task(), name=str(self.device_id)+"periodic")

        except Exception as e:
            logging.error("Error Registering to resources! ")
            logging.error(e)

    def on_data_changed(self, updated_value, **kwargs):
        """
        When a sensor/actuator changes value/state
        a TelemetryMessage is composed looking for
        a needed param and some optional but always given params
        Creates task to publish data
        :param updated_value:
        :param kwargs: type
        :param kwargs: unit
        :return:
        """
        try:
            type = kwargs.get('type', None)
            unit = kwargs.get('unit', None)
            message = TelemetryMessage(type=type, value=updated_value, unit=unit, name=self.device_id)
            asyncio.get_event_loop().create_task(self.publish_telemetry_data(
                topic=f"{mqttParams.MQTT_DEFAULT_TOPIC}/"
                      f"{self.location}/"
                      f"{self.plant}/"
                      f"{self.type}/"
                      f"{mqttParams.DEVICE_TOPIC}/"
                      f"{self.device_id}/"
                      f"{mqttParams.TELEMETRY_TOPIC}",
                message=message.build_senml_json_payload()
            ))
        except Exception as e:
            logging.error("Error! No data received")
            logging.error(e)

    async def publish_telemetry_data(self, topic, message):
        """
        Publish given message on given topic
        :param topic:
        :param message:
        :return:
        """
        # TODO add is connected check
        if topic and message:
            try:
                await self.mqtt_client.publish(topic, message)
                logging.info(f"Data {message} successfully published to {topic}")
            except Exception as e:
                logging.error("Error! Could not publish data")
                logging.error(e)
        else:
            logging.error("Error! msg != None or topic != None or mqttClient not connected")