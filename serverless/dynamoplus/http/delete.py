#from host.http.utils.hostschema import Validator
from dynamoplus.http.handler import HttpHandler
#from jsonschema import ValidationError,SchemaError
import logging
import os
logging.basicConfig(level=logging.INFO)

def delete(event, context):
    entities = os.environ['ENTITIES']
    dynamodbTable = os.environ['DYNAMODB_DOMAIN_TABLE']
    handler = HttpHandler()
    return handler.delete(event["pathParameters"])