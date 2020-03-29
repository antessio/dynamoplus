# from jsonschema import ValidationError,SchemaError
from aws.http.handler.handler import HttpHandler
import logging

logging.basicConfig(level=logging.INFO)
handler = HttpHandler()


def get(event, context):
    return handler.get(event['pathParameters'])


def query(event, context):
    return handler.query(event['pathParameters'], query_string_parameters=event['queryStringParameters'],
                         body=event["body"], headers=event['headers'])


def create(event, context):
    return handler.create(event["pathParameters"], body=event["body"])


def update(event, context):
    return handler.update(event["pathParameters"], body=event["body"])


def delete(event, context):
    return handler.delete(event["pathParameters"])
