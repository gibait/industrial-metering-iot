class MqttConfigurationParams(object):
    BROKER_ADDRESS = "127.0.0.1"
    BROKER_PORT = 1883
    # TODO add authentication configuration by certificate for consumer
    # TODO add authorization configuration for consumer
    MQTT_DEFAULT_TOPIC = "metering"
    DEVICE_TOPIC = "device"
    TELEMETRY_TOPIC = "telemetry"
    CONTROL_TOPIC = "control"
    INFO_TOPIC = "info"