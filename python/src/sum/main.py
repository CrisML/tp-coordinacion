import os
import logging
import threading
import zlib
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

EOF_QUEUE_PREFIX = os.environ.get("EOF_QUEUE_PREFIX", "sum_eof")
PART_QUEUE_PREFIX = os.environ.get("PART_QUEUE_PREFIX", "sum_in")


def _partition_for_fruit(fruit: str, mod: int) -> int:
    return zlib.crc32(fruit.encode("utf-8")) % mod


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

        self.part_queue_name = f"{PART_QUEUE_PREFIX}_{ID}"
        self.part_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, self.part_queue_name)

        self.eof_queue_name = f"{EOF_QUEUE_PREFIX}_{ID}"
        self.eof_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, self.eof_queue_name)

        self.agg_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            for i in range(AGGREGATION_AMOUNT)
        ]

        self._partition_queues = [
            middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, f"{PART_QUEUE_PREFIX}_{pid}")
            for pid in range(SUM_AMOUNT)
        ]

        self._eof_queues = [
            middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, f"{EOF_QUEUE_PREFIX}_{pid}")
            for pid in range(SUM_AMOUNT)
        ]

        self.amount_by_fruit = {}
        self.closed = set()
        self._stop_event = threading.Event()

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info(f"[sum {ID}] SIGTERM received, shutting down")
        self._stop_event.set()
        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass
        try:
            self.part_queue.stop_consuming()
        except Exception:
            pass
        try:
            self.eof_queue.stop_consuming()
        except Exception:
            pass

        for q in getattr(self, "_partition_queues", []):
            try:
                q.close()
            except Exception:
                pass
        for q in getattr(self, "_eof_queues", []):
            try:
                q.close()
            except Exception:
                pass
        for ex in getattr(self, "agg_exchanges", []):
            try:
                ex.close()
            except Exception:
                pass

        try:
            self.input_queue.close()
        except Exception:
            pass
        try:
            self.part_queue.close()
        except Exception:
            pass
        try:
            self.eof_queue.close()
        except Exception:
            pass

    def _process_data(self, query_id, fruit, amount):
        if query_id in self.closed:
            return
        per_query = self.amount_by_fruit.setdefault(query_id, {})
        per_query[fruit] = per_query.get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(
            fruit, int(amount)
        )

    def _flush_query(self, query_id):
        per_query = self.amount_by_fruit.get(query_id, {})

        for final_fruit_item in per_query.values():
            agg_id = _partition_for_fruit(final_fruit_item.fruit, AGGREGATION_AMOUNT)
            payload = message_protocol.internal.serialize(
                [query_id, final_fruit_item.fruit, final_fruit_item.amount]
            )
            self.agg_exchanges[agg_id].send(payload)

        eof_payload = message_protocol.internal.serialize([query_id])
        for ex in self.agg_exchanges:
            ex.send(eof_payload)

        self.amount_by_fruit.pop(query_id, None)

    def _handle_eof_for_me(self, query_id):
        if query_id in self.closed:
            return
        self.closed.add(query_id)
        logging.info(f"[sum {ID}] flushing q={query_id}")
        self._flush_query(query_id)

    def _send_to_partition(self, partition_id: int, message_obj):
        self._partition_queues[partition_id].send(message_protocol.internal.serialize(message_obj))

    def _broadcast_partition_eof(self, query_id: str):
        for pid in range(SUM_AMOUNT):
            self._send_to_partition(pid, ["__EOF_PART__", query_id])

        payload = message_protocol.internal.serialize([query_id])
        for pid in range(SUM_AMOUNT):
            self._eof_queues[pid].send(payload)

    def process_dispatch_message(self, message, ack, nack):
        """Consume desde input_queue y distribuye por partición."""
        try:
            fields = message_protocol.internal.deserialize(message)

            if isinstance(fields, list) and len(fields) == 3 and fields[1] != "__EOF_BROADCAST__":
                query_id, fruit, amount = fields
                pid = _partition_for_fruit(fruit, SUM_AMOUNT)
                self._send_to_partition(pid, [query_id, fruit, amount])
                ack()
                return

            if isinstance(fields, list) and len(fields) == 2 and fields[1] == "__EOF_BROADCAST__":
                query_id = fields[0]
                logging.info(f"[sum {ID}] dispatch EOF_BROADCAST q={query_id}")
                self._broadcast_partition_eof(query_id)
                ack()
                return

            logging.error(f"Invalid message format at Sum(dispatch): {fields}")
            ack()
        except Exception as e:
            logging.error(e)
            nack(requeue=True)

    def process_partition_message(self, message, ack, nack):
        """Consume desde sum_in_{ID} (su partición)."""
        try:
            fields = message_protocol.internal.deserialize(message)

            if isinstance(fields, list) and len(fields) == 3:
                query_id, fruit, amount = fields
                self._process_data(query_id, fruit, amount)
                ack()
                return

            if isinstance(fields, list) and len(fields) == 2 and fields[0] == "__EOF_PART__":
                _, query_id = fields
                self._handle_eof_for_me(query_id)
                ack()
                return

            logging.error(f"Invalid message format at Sum(part): {fields}")
            ack()
        except Exception as e:
            logging.error(e)
            nack(requeue=True)

    def process_eof_message(self, message, ack, nack):
        try:
            ack()
        except Exception:
            nack(requeue=True)

    def start(self):
        t = threading.Thread(
            target=self.eof_queue.start_consuming,
            args=(self.process_eof_message,),
            daemon=True,
        )
        t.start()

        tp = threading.Thread(
            target=self.part_queue.start_consuming,
            args=(self.process_partition_message,),
            daemon=True,
        )
        tp.start()

        if ID == 0:
            logging.info(f"[sum {ID}] acting as dispatcher for {INPUT_QUEUE}")
            self.input_queue.start_consuming(self.process_dispatch_message)
        else:
            self._stop_event.wait()


def main():
    logging.basicConfig(level=logging.INFO)
    SumFilter().start()
    return 0


if __name__ == "__main__":
    main()
