import os
import logging
logging.basicConfig(level=logging.INFO)
from dynamoplus.http.handler import HttpHandler

def get(event, context):
    entities = os.environ['entities']
    dynamodbTable = os.environ['DYNAMODB_TABLE']
    handler = HttpHandler(entities,dynamodbTable)
    return handler.get(event['pathParameters'])