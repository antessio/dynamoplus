import json
import logging
from dynamoplus import dynamo_plus_v2
from aws.dynamodb.dynamodb_repository import DynamoDBRepository
VERSION = "0.5.0.5"
logging.basicConfig(level=logging.INFO)

system_repository = DynamoDBRepository('system')
domain_repository = DynamoDBRepository('domain')
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