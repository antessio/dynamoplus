import json
import logging
import os
from dynamoplus import dynamo_plus_v2
from aws.dynamodb.dynamodb_repository import DynamoDBRepository
VERSION = "0.5.0.5"
logging.basicConfig(level=logging.INFO)

SYSTEM_TABLE_NAME = os.environ['SYSTEM_TABLE_NAME']
DOMAIN_TABLE_NAME = os.environ['DOMAIN_TABLE_NAME']
system_repository = DynamoDBRepository(SYSTEM_TABLE_NAME)
domain_repository = DynamoDBRepository(DOMAIN_TABLE_NAME)

dynamoplus = dynamo_plus_v2.Dynamoplus(
    system_repository,
    system_repository,
    system_repository,
    system_repository,
    system_repository,
    domain_repository
)

def info(event, context):
    system_info = {"version": dynamoplus.info()}
    return {"statusCode": 200, "body": json.dumps(system_info)}