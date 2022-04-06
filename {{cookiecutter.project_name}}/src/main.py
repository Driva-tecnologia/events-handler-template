from consumer import Consumer
from handler import Handler
from models.{{cookiecutter.consume_topic}}_model import {{cookiecutter.message_class}}
from producer import Producer


def main():
    CONSUME_TOPIC = "{{cookiecutter.consume_topic}}"
    PRODUCE_TOPIC = "{{cookiecutter.produce_topic}}"
    c = Consumer({{cookiecutter.message_class}}, CONSUME_TOPIC)
    p = Producer({{cookiecutter.message_class}}, PRODUCE_TOPIC)
    h = Handler()
    for msg in c.consume({{cookiecutter.message_class}}, limit=1):
        msg_handled = h.handle(msg.copy())
        if msg_handled != msg:
            print("Change detected")
            p.send(msg_handled)


if __name__ == "__main__":
    main()
