import json

from typing import Generator, Type, TypeVar
from uuid import uuid4

from confluent_kafka import DeserializingConsumer
from confluent_kafka.error import ConsumeError
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

from kafka_utils import get_conf, get_schema_registry_client, pop_schema_registry_params
from models.base_model import BaseModel

MessageType = TypeVar("MessageType", bound=BaseModel)


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
        self, _: Type[MessageType], limit: int = None
    ) -> Generator[MessageType, None, None]:
        """Consumes all the messages from the topic."""
        messages_consumed = 0
        while True:
            try:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # No message available within timeout.
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting for message or event/error in poll()")
                    continue
                if msg.error():
                    print(f"error: {msg.error()}")
                else:
                    value = msg.value()
                    yield value
                    messages_consumed += 1
                    if limit and messages_consumed == limit:
                        break
            except KeyboardInterrupt:
                break
            except ConsumeError as exc:
                # Report malformed record, discard results, continue polling
                print(exc.code, exc.name, exc.exception)
                print(f"Message deserialization failed {exc}")
                break

    def __del__(self) -> None:
        self.consumer.close()
