import os
# from host.http.utils.hostschema import Validator
from dynamoplus.http.handler.handler import HttpHandler
# from jsonschema import ValidationError,SchemaError
import logging

logging.basicConfig(level=logging.INFO)


def update(event, context):
    handler = HttpHandler()
    return handler.update(event["pathParameters"], body=event["body"])
