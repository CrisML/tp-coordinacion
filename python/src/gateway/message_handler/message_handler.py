import uuid
from common import message_protocol


class MessageHandler:
    def __init__(self):
        self.query_id = uuid.uuid4().hex

    def serialize_data_message(self, message):
        fruit, amount = message
        return message_protocol.internal.serialize([self.query_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.query_id, "__EOF_BROADCAST__"])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if not isinstance(fields, list) or len(fields) != 2:
            return None
        if fields[0] != self.query_id:
            return None
        return fields[1]
