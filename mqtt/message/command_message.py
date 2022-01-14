from generic_message import GenericMessage

class CommandMessage(GenericMessage):
    def __init__(self, type, metadata):
        super().__init__(type, metadata)