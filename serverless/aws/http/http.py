# from jsonschema import ValidationError,SchemaError
from aws.http.handler.handler import HttpHandler
import logging

logging.basicConfig(level=logging.INFO)
handler = HttpHandler()


def get(event, context):
    return handler.get(path_parameters=event['pathParameters'],
                         query_string_parameters=event['queryStringParameters'],
                         body=event["body"], headers=event['headers'])


def query(event, context):
    return handler.query(path_parameters=event['pathParameters'],
                         query_string_parameters=event['queryStringParameters'],
                         body=event["body"], headers=event['headers'])


def create(event, context):
    return handler.create(path_parameters=event['pathParameters'],
                         query_string_parameters=event['queryStringParameters'],
                         body=event["body"], headers=event['headers'])


def update(event, context):
    return handler.update(path_parameters=event['pathParameters'],
                         query_string_parameters=event['queryStringParameters'],
                         body=event["body"], headers=event['headers'])


def delete(event, context):
    return handler.delete(path_parameters=event['pathParameters'],
                         query_string_parameters=event['queryStringParameters'],
                         body=event["body"], headers=event['headers'])
