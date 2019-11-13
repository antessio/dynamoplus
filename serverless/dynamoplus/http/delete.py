# from host.http.utils.hostschema import Validator
from dynamoplus.http.handler.handler import HttpHandler
# from jsonschema import ValidationError,SchemaError
import logging
import os

logging.basicConfig(level=logging.INFO)


def delete(event, context):
    handler = HttpHandler()
    return handler.delete(event["pathParameters"])
