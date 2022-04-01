# Getting the model
The messages on all topics in Kafka have a [schema](https://docs.confluent.io/platform/current/schema-registry/index.html) defined in [Avro](https://avro.apache.org/docs/1.2.0).

You can generate a [Pydantic](https://pydantic-docs.helpmanual.io/) model from a schema to use in your code using the `Consumer.generate_model` class method and pass it the topic to get the schema for.

This will create a file with the topic name for the model representing the messages.
If it has nested models those will also appear in the same file.

After you run the class method instructions will be presented in the terminal to ajust the file for your use.
**You can remove any unused import after.**

By default a running the setup.py script will do all that automatically.
