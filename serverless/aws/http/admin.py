import logging

## TODO: use an interface and not directly the repo
from dynamoplus.repository.repositories import create_tables


logging.basicConfig(level=logging.INFO)


def setup(event, context):
    create_tables()
    return {"statusCode": 200}