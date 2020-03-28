# from jsonschema import ValidationError,SchemaError
from aws.http.handler.handler import HttpHandler
from dynamoplus.dynamo_plus import get as dynamoplus_get,update as dynamoplus_update,query as dynamoplus_query,create as dynamoplus_create,delete as dynamoplus_delete
import logging

logging.basicConfig(level=logging.INFO)
handler = HttpHandler()


def get(event, context):
    return dynamoplus_get(event['pathParameters'])


def query(event, context):
    return dynamoplus_query(event['pathParameters'], query_string_parameters=event['queryStringParameters'],
                         body=event["body"], headers=event['headers'])


def create(event, context):
    return dynamoplus_create(event["pathParameters"], body=event["body"])


def update(event, context):
    return dynamoplus_update(event["pathParameters"], body=event["body"])


def delete(event, context):
    return dynamoplus_delete(event["pathParameters"])
