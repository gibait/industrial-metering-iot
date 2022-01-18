import asyncio
import json
import uuid
import logging
import paho.mqtt.client as mqtt

from asyncio_mqtt import Client
from mqtt.conf.mqtt_conf_params import MqttConfigurationParams as mqttParams
from mqtt.message.telemetry_message import TelemetryMessage
from mqtt.message.control_message import ControlMessage
from mqtt.resource.water_sensor_resource import WaterSensorResource
from mqtt.resource.gas_sensor_resource import GasSensorResource

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)-8s %(message)s")

class GenericResourceConsumer:
    """
        Generic Resource Consumer class handling every kind of resource
        Load threshold data from a file
        Is started by an async start function
        Uses logging
    """

    # Load delay times from json config file
    with open('../conf/config_values.json') as json_file:
        config_values = json.load(json_file)

    is_resource_threshold_notified = {}

    def __init__(self):
        self.id = str(uuid.uuid4())
        self.resource_value_history = {}
        # Using Client class from async-mqtt wrapper
        self.mqtt_client = Client(
            hostname=mqttParams.BROKER_ADDRESS,
            port=mqttParams.BROKER_PORT,
            clean_session=True,
            client_id=str(self.id),
            keepalive=10
        )

    async def on_connect(self):
        """
            Connect to mqtt broker
        """
        try:
            await self.mqtt_client.connect()
            logging.info(f"Consumer {self.id} successfully connected to MQTT Broker")
        except Exception as e:
            logging.error("Error establishing connection!")
            logging.error(e)

    async def subscribe_to_telemetry_topic(self):
        """
            Subscribe to every telemetry topic available using mqtt wildcards
            /metering/[resource]/device/[unique_id]/(control)(telemetry)
        """
        try:
            # Subscribe to all telemetry topic available
            device_telemetry_topic = "{0}/+/{1}/+/{2}".format(
                mqttParams.MQTT_DEFAULT_TOPIC,
                mqttParams.DEVICE_TOPIC,
                mqttParams.TELEMETRY_TOPIC
            )
            await self.mqtt_client.subscribe(device_telemetry_topic)
            logging.info("Subscribed to: " + device_telemetry_topic)
        except Exception as e:
            logging.error("Error subscribing to topic!")
            logging.error(e)


    async def on_message(self):
        """
            Iterative core function of the program
            Compose TelemetryMessage and checks if allowed type
            Store new values for threshold checks
            Delay resume control message when threshold is reached
            Completely asynchronous
        """
        try:
            # Iterate through messages
            async with self.mqtt_client.filtered_messages('#') as messages:
                async for message in messages:
                    # Craft Telemetry message
                    telemetry_message:TelemetryMessage = self.parse_telemetry_message(message)
                    # Check for the resource type
                    if telemetry_message.type == WaterSensorResource.RESOURCE_TYPE or telemetry_message.type == GasSensorResource.RESOURCE_TYPE:
                        new_level = telemetry_message.value
                        logging.info(f"Detected new value: {new_level} in topic {message.topic}")

                        if message.topic not in self.resource_value_history:
                            logging.info(f"New level saved for {message.topic}")
                            # Storing values by topic because unique identifier
                            self.resource_value_history[message.topic] = new_level
                            self.is_resource_threshold_notified[message.topic] = False
                        else:
                            if self.is_level_threshold(self.resource_value_history.get(message.topic), new_level, telemetry_message.type) and not self.is_resource_threshold_notified[message.topic]:
                                logging.info(f"{message.topic} THRESHOLD LEVEL REACHED! Sending control notification ...")
                                # Resource is now notified
                                self.is_resource_threshold_notified[message.topic] = True

                                # Topic string manipulation by adding control to it
                                control_topic = self.refactor_telemetry_topic_to_control(message)

                                await self.publish_control_message(control_topic, ControlMessage('alert', {message.topic:"threshold_reached"}))
                                # Resetting resource value history
                                self.resource_value_history.pop(message.topic, None)

                                # Retrieve delay based on sensor type
                                delay = self.config_values[telemetry_message.type]["RESTART_DELAY"]
                                # Safely delay resume control message with async
                                asyncio.run_coroutine_threadsafe(self.delay_control_message(delay, control_topic), asyncio.get_event_loop())

        except Exception as e:
            logging.error("Error receiving message!")
            logging.error(e)

    def refactor_telemetry_topic_to_control(self, message):
        """
            Strip telemetry from topic and append control
        """
        try:
            topic = message.topic.split('/')[:-1]
            topic.append(mqttParams.CONTROL_TOPIC)
            control_topic = '/'.join(topic)
            return control_topic
        except Exception as e:
            logging.error("Error refactoring topic")
            logging.error(e)

    async def delay_control_message(self, delay, topic):
        """
            Await given time and publish a resume control message
        """
        try:
            await asyncio.sleep(delay)
            logging.info(f"{topic} DELAY IS OVER, sending resume control notification")
            await self.publish_control_message(topic, ControlMessage('alert', {topic: "resume_operation"}))
        except Exception as e:
            logging.error("Error delaying resume control message")
            logging.error(e)

    def is_level_threshold(self, last_level, new_level, type):
        return new_level - last_level >= self.config_values[type]["SUPPLY_THRESHOLD"]

    def parse_telemetry_message(self, mqtt_message:mqtt.MQTTMessage):
        """
            Compose TelemetryMessage using mqtt message
        """
        try:
            if isinstance(mqtt_message, mqtt.MQTTMessage):
                payload = json.loads(mqtt_message.payload.decode("utf-8"))
                # Crafting Telemetry message for better parsing
                return TelemetryMessage(**payload)
        except Exception as e:
            logging.error("Error! Cannot parse message")
            logging.error(e)

    async def publish_control_message(self, topic:str, message:ControlMessage):
        """
            Publish control message to control topic
        """
        # TODO add is connected check
        if topic and message:
            try:
                await self.mqtt_client.publish(topic, message.to_json())
                logging.info(f"Data {message.to_json()} successfully published to {topic}")
            except Exception as e:
                logging.error("Error! Could not publish message")
                logging.error(e)
        else:
            logging.error("Error! msg != None or topic != None or mqttClient not connected")

    async def start(self):
        try:
            await self.on_connect()
            await self.subscribe_to_telemetry_topic()
            asyncio.get_event_loop().create_task(self.on_message())
            logging.info("Data Manager started ...")
        except Exception as e:
            logging.error("Error starting Data Manager!")
            logging.error(e)

def main():
    asyncio.get_event_loop().create_task(GenericResourceConsumer().start())
    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    main()