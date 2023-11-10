import logging
import os
import json
from aws.dynamodb.dynamodb_repository import DynamoDBRepository
logging.basicConfig(level=logging.INFO)
SYSTEM_TABLE_NAME = os.environ['SYSTEM_TABLE_NAME']
DOMAIN_TABLE_NAME = os.environ['DOMAIN_TABLE_NAME']
system_repository = DynamoDBRepository(SYSTEM_TABLE_NAME)
domain_repository = DynamoDBRepository(DOMAIN_TABLE_NAME)

def setup(event, context):
    system_repository.create_table()
    domain_repository.create_table()
    return {"statusCode": 201, "body": json.dumps({"message": "ok"})}

def cleanup(event, context):
    system_repository.cleanup_table()
    domain_repository.cleanup_table()
    return {"statusCode": 201, "body": json.dumps({"message": "ok"})}
