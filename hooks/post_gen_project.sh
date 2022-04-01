#!/usr/bin/bash

poetry install
if poetry run python src/setup.py ; then
    git init 
    git config user.email "{{cookiecutter.author_email}}"
    git config user.name "{{cookiecutter.author_name}}"
    git add .
    git commit -m "first commit"
    poetry run pre-commit install
else
    echo "Error: setup.py failed"
    exit 1
fi