import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

        self.fruit_list = {}
        self.eof_count = {}

    def _process_data(self, query_id, fruit, amount):
        lst = self.fruit_list.setdefault(query_id, [])
        for i in range(len(lst)):
            if lst[i].fruit == fruit:
                lst[i] = lst[i] + fruit_item.FruitItem(fruit, amount)
                return
        bisect.insort(lst, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, query_id):
        c = self.eof_count.get(query_id, 0) + 1
        self.eof_count[query_id] = c
        logging.info(f"[aggregation {ID}] EOF q={query_id} count={c}/{SUM_AMOUNT}")
        if c < SUM_AMOUNT:
            return

        lst = self.fruit_list.get(query_id, [])
        chunk = list(lst[-TOP_SIZE:])
        chunk.reverse()
        top = [(fi.fruit, fi.amount) for fi in chunk]

        self.output_queue.send(message_protocol.internal.serialize([query_id, top]))
        logging.info(f"[aggregation {ID}] FINISH q={query_id} sending top")

        self.fruit_list.pop(query_id, None)
        self.eof_count.pop(query_id, None)

    def process_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)

            if isinstance(fields, list) and len(fields) == 3:
                query_id, fruit, amount = fields
                self._process_data(query_id, fruit, amount)
                ack()
                return

            if isinstance(fields, list) and len(fields) == 1:
                query_id = fields[0]
                self._process_eof(query_id)
                ack()
                return

            logging.error(f"Invalid message format at Aggregation: {fields}")
            ack()
        except Exception as e:
            logging.error(e)
            nack(requeue=True)

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    AggregationFilter().start()
    return 0


if __name__ == "__main__":
    main()
