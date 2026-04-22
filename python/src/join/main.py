import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

        self.count = {}
        self.acc = {}

    def _merge_partial(self, query_id, partial_top):
        acc = self.acc.setdefault(query_id, {})
        for fruit, amount in partial_top:
            item = fruit_item.FruitItem(fruit, amount)
            acc[fruit] = acc.get(fruit, fruit_item.FruitItem(fruit, 0)) + item

    def _maybe_finish(self, query_id):
        if self.count.get(query_id, 0) < AGGREGATION_AMOUNT:
            return

        items = sorted(self.acc.get(query_id, {}).values())
        items.reverse()
        final = [(fi.fruit, fi.amount) for fi in items[:TOP_SIZE]]

        self.output_queue.send(message_protocol.internal.serialize([query_id, final]))

        self.count.pop(query_id, None)
        self.acc.pop(query_id, None)

    def process_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if not (isinstance(fields, list) and len(fields) == 2):
                logging.error(f"Invalid message format at Join: {fields}")
                ack()
                return

            query_id, partial_top = fields
            self._merge_partial(query_id, partial_top)

            self.count[query_id] = self.count.get(query_id, 0) + 1
            self._maybe_finish(query_id)

            ack()
        except Exception as e:
            logging.error(e)
            nack(requeue=True)

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    JoinFilter().start()
    return 0


if __name__ == "__main__":
    main()
