class MqttConfigurationParams(object):
    BROKER_ADDRESS = "127.0.0.1"
    BROKER_PORT = 1883
    # TODO add authentication configuration
    MQTT_DEFAULT_TOPIC = "metering"
    WATER_DEVICE_TOPIC = "water/device"
    TELEMETRY_TOPIC = "telemetry"
    COMMAND_TOPIC = "command"