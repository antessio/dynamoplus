import os
import logging
logging.basicConfig(level=logging.INFO)
from dynamoplus.http.handler import HttpHandler

def query(event, context):
    handler = HttpHandler()
    return handler.query(event['pathParameters'], query_string_parameters=event['queryStringParameters'], body=event["body"], headers=event['headers'])
