from kpn_senml import SenmlUnits

class WaterConfValues:
    RESOURCE_TYPE = "iot:sensor:water"
    # WATER Values
    MAX_USAGE = 30.0
    MIN_USAGE = 80.0
    MAX_INCREASE = 1.0
    MIN_INCREASE = -0.5
    TASK_DELAY = 2
    UPDATE_DELAY = 2
    UNIT = SenmlUnits.SENML_UNIT_LITER_PER_SECOND

class GasConfValues:
    RESOURCE_TYPE = "iot:sensor:gas"
    # GAS Values
    MAX_USAGE = 200.0
    MIN_USAGE = 100.0
    MAX_INCREASE = 1.0
    MIN_INCREASE = -0.5
    TASK_DELAY = 4
    UPDATE_DELAY = 3
    UNIT = SenmlUnits.SENML_UNIT_KILOGRAM

class ElectricityConfValues:
    RESOURCE_TYPE = "iot:sensor:electricity"
    # ELECTRICITY Values
    MAX_USAGE = 20.0
    MIN_USAGE = 10.0
    MAX_INCREASE = 1.0
    MIN_INCREASE = -0.5
    TASK_DELAY = 6
    UPDATE_DELAY = 5
    UNIT = "kWh"