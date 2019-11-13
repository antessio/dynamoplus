import os
import logging
logging.basicConfig(level=logging.INFO)
from dynamoplus.http.handler.handler import HttpHandler

def get(event, context):
    handler = HttpHandler()
    return handler.get(event['pathParameters'])