import json
from typing import Type

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from kafka_utils import (
    create_topic,
    get_conf,
    get_schema_registry_client,
    pop_schema_registry_params,
)
from models.base_model import BaseModel


class Producer:
    def __init__(self, cls: Type[BaseModel], topic: str) -> None:
        schema_registry_client = get_schema_registry_client()

        schema_str = json.dumps(cls.avro_schema())

        serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=schema_str,
            to_dict=cls.serialize,
        )

        producer_conf = pop_schema_registry_params(get_conf())
        producer_conf["key.serializer"] = StringSerializer()
        producer_conf["value.serializer"] = serializer

        self.producer = SerializingProducer(producer_conf)
        self.topic = topic

        create_topic(topic)

    def send(self, data: BaseModel):
        self.producer.produce(topic=self.topic, key=data.get_key(), value=data)
        self.producer.poll(0)

    def __del__(self):
        self.producer.flush()
