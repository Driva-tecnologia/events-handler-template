"""Module for kafka utils."""
import json
import os
import sys
from pathlib import Path
from textwrap import dedent
from typing import Dict

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from dotenv import load_dotenv
from pydantic_avro.avro_to_pydantic import avsc_to_pydantic


def get_conf() -> Dict[str, str]:
    """Returns a dict with the kafka configuration, based on env vars."""
    load_dotenv(dotenv_path=".env.example", override=True)
    base_conf = {
        "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
        "schema.registry.url": os.environ["SCHEMA_REGISTRY_URL"],
    }
    username = os.getenv("SASL_USERNAME")
    password = os.getenv("SASL_PASSWORD")
    schema_registry_auth = os.getenv("SCHEMA_REGISTRY_AUTH")

    if username and password and schema_registry_auth:
        auth_conf = {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": username,
            "sasl.password": password,
            "basic.auth.user.info": schema_registry_auth,
        }
        base_conf.update(auth_conf)
    return base_conf


def pop_schema_registry_params(conf: Dict[str, str]) -> Dict[str, str]:
    """Removes Schema Registry related configuration.

    Args:
        conf: The configuration if potential Schema Registry related params.
    """
    conf.pop("schema.registry.url", None)
    conf.pop("basic.auth.user.info", None)
    conf.pop("basic.auth.credentials.source", None)

    return conf


def create_topic(topic_name: str):
    """Creates the topic if needed.

    Args:
        topic_name: The name of the topic
    """
    admin_client_conf = pop_schema_registry_params(get_conf())

    admin_client = AdminClient(admin_client_conf)

    futures = admin_client.create_topics(
        [
            NewTopic(
                topic_name,
                num_partitions=6,
                replication_factor=1,
            )
        ]
    )

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic {topic} created")
        except KafkaException as exc:
            code = exc.args[0].code()
            # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
            # Otherwise fail fast
            if code != KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"Failed to create topic {topic}: {exc}")
                sys.exit(1)


def get_schema_registry_client() -> SchemaRegistryClient:
    """TODO"""
    conf = get_conf()
    schema_registry_conf = {
        "url": conf["schema.registry.url"],
    }
    if "basic.auth.user.info" in conf:
        schema_registry_conf["basic.auth.user.info"] = conf["basic.auth.user.info"]

    return SchemaRegistryClient(schema_registry_conf)


def get_schema(topic: str) -> str:
    """TODO"""
    schema_registry_client = get_schema_registry_client()
    schema = schema_registry_client.get_latest_version(topic + "-value")
    schema_str = schema.schema.schema_str
    return schema_str


def generate_model(topic: str) -> None:
    """TODO"""
    schema_str = get_schema(topic)
    schema_dict = json.loads(schema_str)
    file_content = avsc_to_pydantic(schema_dict)

    filename = f"{topic}_model.py"
    path = Path(__file__).parent / "models" / filename
    with open(path, "w", encoding="utf-8") as file:
        file.write(file_content)
    print()
    print(f"File with class representing messages on topic {topic} created.")
    print("Follow this steps to make the code work:")
    steps = dedent(
        """
    1) Go to the src/models/{{cookiecutter.consume_topic}}_model.py file
    
    2) Replace the 'from pydantic import BaseModel' for 'from models.base_model import BaseModel'

    3) Implement the get_key method on the models.
    It should return a string to uniquely identify the message.
    In general the field that is required is a good option, since the key cannot be null/None.
    """
    )
    print(steps)
