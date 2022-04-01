from kafka_utils import generate_model


def main():
    CONSUME_TOPIC = "{{cookiecutter.consume_topic}}"
    generate_model(CONSUME_TOPIC)


if __name__ == "__main__":
    main()
