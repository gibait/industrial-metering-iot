from mqtt.message.generic_message import GenericMessage

class ControlMessage(GenericMessage):
    def __init__(self, type, metadata, **kwargs):
        super().__init__(type, metadata, **kwargs)