import uuid
import logging

import asyncio
from mqtt.device.metering_smart_object import MeteringSmartObject
from mqtt.resource.water_sensor_resource import WaterSensorResource
from mqtt.resource.gas_sensor_resource import GasSensorResource
from mqtt.resource.generic_actuator_resource import GenericActuatorResource

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)-8s %(message)s")

class MeteringSmartObjectProcess:
    def __init__(self):
        self.device_id = uuid.uuid4()

        try:
            water_metering_smart_object = MeteringSmartObject({
                "sensor" : WaterSensorResource(),
                "actuator" : GenericActuatorResource()
            }, "water")
            asyncio.get_event_loop().create_task(water_metering_smart_object.start())

            gas_metering_smart_object = MeteringSmartObject({
                "sensor": GasSensorResource(),
                "actuator": GenericActuatorResource()
            }, "gas")
            asyncio.get_event_loop().create_task(gas_metering_smart_object.start())

            asyncio.get_event_loop().run_forever()

        except Exception as e:
            logging.info(e)

def main():
    MeteringSmartObjectProcess()

if __name__ == '__main__':
    main()