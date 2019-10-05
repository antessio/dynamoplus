import os
#from host.http.utils.hostschema import Validator
from dynamoplus.http.handler import HttpHandler
#from jsonschema import ValidationError,SchemaError
import logging

logging.basicConfig(level=logging.INFO)

def create(event, context):
    handler = HttpHandler()
    return handler.create(event["pathParameters"],body=event["body"])