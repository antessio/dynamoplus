# from jsonschema import ValidationError,SchemaError
from aws.http.handler.handler import HttpHandler
import logging

logging.basicConfig(level=logging.INFO)
handler = HttpHandler()


def get(event, context):
    return get(event['pathParameters'])


def query(event, context):
    return query(event['pathParameters'], query_string_parameters=event['queryStringParameters'],
                         body=event["body"], headers=event['headers'])


def create(event, context):
    return create(event["pathParameters"], body=event["body"])


def update(event, context):
    return update(event["pathParameters"], body=event["body"])


def delete(event, context):
    return handler.delete(event["pathParameters"])
