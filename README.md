# Python Kafka Event Handler Cookiecutter
[Cookiecutter]((https://github.com/audreyr/cookiecutter)) template for a handler of [Kafka](https://kafka.apache.org/) events/messages 

# Features
- Formatting with [black](https://github.com/psf/black)
- Import sorting with [isort](https://github.com/timothycrosley/isort)
- Static typing with [mypy](http://mypy-lang.org/)
- Linting with [flake8](http://flake8.pycqa.org/en/latest/)
- Linting with [pylint](https://pylint.pycqa.org/en/latest/)
- Git hooks that run all the above with [pre-commit](https://pre-commit.com/)
- Modern dependecy management with [poetry](https://python-poetry.org/)

# How to use
```sh
# Install cookiecutter if not available
pip install cookiecutter
# Or
pip3 install cookiecutter

# Install poetry if not available
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 -

# Use cookiecutter to create the project from this template
cookiecutter gh:Driva-tecnologia/events-handler-template

# Enter the project directory
cd <project_name>

# Spawn a shell with a virtual environment
poetry shell

# Start coding ...

```

# Credits
Based on the [sourcery-ai/python-best-practices-cookiecutter](https://github.com/sourcery-ai/python-best-practices-cookiecutter) project template.
