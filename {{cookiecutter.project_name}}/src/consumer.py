import json
from typing import Generator, List, Tuple, Type, TypeVar
from uuid import uuid4

from confluent_kafka import (
    TIMESTAMP_NOT_AVAILABLE,
    DeserializingConsumer,
    TopicPartition,
)
from confluent_kafka.error import ConsumeError
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from kafka_utils import get_conf, get_schema_registry_client, pop_schema_registry_params
from models.base_model import BaseModel

MessageType = TypeVar("MessageType", bound=BaseModel)

ConsumeGenerator = Generator[Tuple[MessageType, int, int], None, None]


class Consumer:
    def __init__(
        self,
        cls: Type[BaseModel],
        topic: str,
        group_id: str = None,
        offset_reset: str = "earliest",
    ) -> None:
        schema_registry_client = get_schema_registry_client()

        schema_str = json.dumps(cls.avro_schema())

        deserializer = AvroDeserializer(
            schema_registry_client=schema_registry_client,
            schema_str=schema_str,
            from_dict=cls.deserialize,
        )

        consumer_conf = pop_schema_registry_params(get_conf())
        consumer_conf["key.deserializer"] = StringDeserializer()
        consumer_conf["value.deserializer"] = deserializer

        if not group_id:
            group_id = "consumer-" + uuid4().hex[:8]

        consumer_conf["group.id"] = group_id
        consumer_conf["auto.offset.reset"] = offset_reset

        self.consumer = DeserializingConsumer(consumer_conf)

        self.consumer.subscribe([topic])

    def consume(
        self,
        _: Type[MessageType],
        offsets: List[int] = None,
        timeout_sec: int = -1,
        max_timeouts: int = -1,
    ) -> ConsumeGenerator:
        """Consumes all the messages from the topic."""
        total_timeouts = 0
        offsets_read = 0
        while True:
            try:
                # Move to offset if offsets are provided
                if offsets:
                    # All offsets are consumed
                    if offsets_read == len(offsets):
                        break

                    partitions = [
                        TopicPartition(
                            topic=p.topic,
                            partition=p.partition,
                            offset=offsets[offsets_read],
                        )
                        for p in self.consumer.assignment()
                    ]
                    self.consumer.assign(partitions)

                    offsets_read += 1

                msg = self.consumer.poll(timeout_sec)
                if msg is None:
                    # No message available within timeout.
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting for message or event/error in poll()")
                    print(
                        f"Timeout ({total_timeouts}/{max_timeouts}) reached after {timeout_sec} seconds"
                    )
                    total_timeouts += 1
                    offsets_read -= 1
                    if total_timeouts >= max_timeouts:
                        break
                    continue
                if msg.error():
                    # Just prints the error and continues
                    print(f"error: {msg.error()}")
                    offsets_read -= 1
                else:
                    # Get the message date and returns it
                    value = msg.value()
                    offset = msg.offset()
                    typ, timestamp = msg.timestamp()
                    if typ == TIMESTAMP_NOT_AVAILABLE:
                        print("Timestamp not available ", timestamp)
                        yield value, 0, offset
                    else:
                        yield value, timestamp, offset
            except KeyboardInterrupt:
                break
            except ConsumeError as exc:
                # Report malformed record, discard results, continue polling
                print(exc.code, exc.name, exc.exception)
                print(f"Message deserialization failed {exc}")
                continue

    def __del__(self) -> None:
        self.consumer.close()
