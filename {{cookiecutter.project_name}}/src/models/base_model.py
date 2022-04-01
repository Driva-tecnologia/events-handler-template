from confluent_kafka.serialization import SerializationContext
from pydantic_avro.base import AvroBase


class BaseModel(AvroBase):
    @staticmethod
    def serialize(obj: "BaseModel", _: SerializationContext) -> dict:
        return obj.dict(exclude_none=True)

    def get_key(self) -> str:
        """Returns the key of the message, should be unique."""
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, obj: dict, _: SerializationContext) -> "BaseModel":
        """Deserializes the dict into the model.

        Called by the Kafka Consumer.
        """
        return cls(**obj)
