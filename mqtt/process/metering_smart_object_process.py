import logging
import asyncio

from collections import defaultdict
from mqtt.device.metering_smart_object import MeteringSmartObject
from mqtt.resource.water_sensor_resource import WaterSensorResource
from mqtt.resource.gas_sensor_resource import GasSensorResource
from mqtt.resource.electricity_sensor_resource import ElectricitySensorResource
from mqtt.resource.generic_actuator_resource import GenericActuatorResource

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)-8s %(message)s")

def nested_dict():
    return defaultdict(nested_dict)

class MeteringSmartObjectProcess:

    device_map = nested_dict()

    def add_device_to_map(self, location, plant, resource, device):
        try:
            if not self.device_map[location][plant][resource]:
                self.device_map[location][plant][resource] = device
                logging.info(f"Device installed to {plant} in {location}")
                return True
            else:
                logging.error(f"A device is already present at {location} on {plant} plant")
                return False
        except Exception as e:
            print(e)

    def add_water_smart_obj(self, location, plant):
        try:
            type = "water"
            water_metering_smart_object = MeteringSmartObject({
                "sensor" : WaterSensorResource(),
                "actuator" : GenericActuatorResource()
            }, type, location, plant)
            if self.add_device_to_map(location, plant, type, water_metering_smart_object):
                asyncio.get_event_loop().create_task(water_metering_smart_object.start())
        except Exception as e:
            logging.error("Error creating Water Metering Smart Object")
            logging.error(e)

    def add_gas_smart_obj(self, location, plant):
        try:
            gas_metering_smart_object = MeteringSmartObject({
                "sensor": GasSensorResource(),
                "actuator": GenericActuatorResource()
            }, "gas", location, plant)
            if self.add_device_to_map(location, plant, "gas", gas_metering_smart_object):
                asyncio.get_event_loop().create_task(gas_metering_smart_object.start())
        except Exception as e:
            logging.error("Error creating Gas Metering Smart Object")
            logging.error(e)

    def add_electricity_smart_obj(self, location, plant):
        try:
            electricity_metering_smart_object = MeteringSmartObject({
                "sensor": ElectricitySensorResource(),
                "actuator": GenericActuatorResource()
            }, "electricity", location, plant)
            if self.add_device_to_map(location, plant, "electricity", electricity_metering_smart_object):
                asyncio.get_event_loop().create_task(electricity_metering_smart_object.start())
        except Exception as e:
            logging.error("Error creating Electricity Metering Smart Object")
            logging.error(e)

def main():
    mqtt_consumer_emulator = MeteringSmartObjectProcess()
    mqtt_consumer_emulator.add_gas_smart_obj("padiglione-01", "impianto01")
    mqtt_consumer_emulator.add_water_smart_obj("padiglione-01", "impianto01")
    mqtt_consumer_emulator.add_electricity_smart_obj("padiglione-01", "impianto01")
    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    main()