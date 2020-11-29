import logging

## TODO: use an interface and not directly the repo
from dynamoplus.v2.repository.repositories import create_tables,cleanup_tables


logging.basicConfig(level=logging.INFO)


def setup(event, context):
    create_tables()
    return {"statusCode": 200}

def cleanup(event,context):
    cleanup_tables()
    return {"statusCode": 200}