#from host.http.utils.hostschema import Validator
from dynamoplus.http.handler import HttpHandler
#from jsonschema import ValidationError,SchemaError
import logging
import os
logging.basicConfig(level=logging.INFO)

def delete(event, context):
    entities = os.environ['entities']
    dynamodbTable = os.environ['DYNAMODB_TABLE']
    handler = HttpHandler(entities,dynamodbTable)
    return handler.delete(event["pathParameters"])