import os
import logging
logging.basicConfig(level=logging.INFO)
from dynamoplus.http.handler import HttpHandler

def query(event, context):
    entities = os.environ['ENTITIES']
    dynamodbTable = os.environ['DYNAMODB_TABLE']
    handler = HttpHandler(entities,dynamodbTable)
    return handler.query(event['pathParameters'],queryStringParameters=event['queryStringParameters'],body=event["body"],headers=event['headers'])
