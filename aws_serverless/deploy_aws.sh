#!/bin/bash

poetry build --format wheel

poetry run pip install --upgrade --only-binary :all: --platform manylinux2010_x86_64 --target package dist/*.whl

#sam build
#
#sam deploy --stack-name dynamoplus-dev