from models.{{cookiecutter.consume_topic}}_model import {{cookiecutter.message_class}}


class Handler:
    def handle(self, message: {{cookiecutter.message_class}}) -> {{cookiecutter.message_class}}:
        return message
