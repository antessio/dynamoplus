import os
import random
from enum import Enum

import boto3
from moto import mock_dynamodb2

from dynamoplus.models.system.aggregation.aggregation import AggregationType, AggregationTrigger


@mock_dynamodb2
def set_up_for_integration_test(table_name: str):
    os.environ["TEST_FLAG"] = "true"
    os.environ["AWS_REGION"] = "eu-west-1"
    os.environ["DYNAMODB_DOMAIN_TABLE"] = table_name
    os.environ["DYNAMODB_SYSTEM_TABLE"] = table_name
    dynamodb = boto3.resource("dynamodb")
    dynamodb.create_table(TableName=table_name,
                          KeySchema=[
                              {'AttributeName': 'pk', 'KeyType': 'HASH'},
                              {'AttributeName': 'sk', 'KeyType': 'RANGE'}
                          ],
                          AttributeDefinitions=[
                              {'AttributeName': 'pk', 'AttributeType': 'S'},
                              {'AttributeName': 'sk', 'AttributeType': 'S'},
                              {'AttributeName': 'data', 'AttributeType': 'S'}
                          ],
                          GlobalSecondaryIndexes=[
                              {
                                  'IndexName': 'sk-data-index',
                                  'KeySchema': [{'AttributeName': 'sk', 'KeyType': 'HASH'},
                                                {'AttributeName': 'data', 'KeyType': 'RANGE'}],
                                  "Projection": {"ProjectionType": "ALL"}
                              }
                          ]
                          )
    table = dynamodb.Table(table_name)


def cleanup_table(table_name: str):
    table = get_dynamodb_table(table_name)
    table.delete()
    if "DYNAMODB_DOMAIN_TABLE" in os.environ:
        del os.environ["DYNAMODB_DOMAIN_TABLE"]
        del os.environ["DYNAMODB_SYSTEM_TABLE"]


def get_dynamodb_table(table_name):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    return table


def random_enum(enum: Enum):
    return random.choice(list(enum))


def random_aggregation_configuration_API_data():
    aggregation_type = random_enum(AggregationType).id
    return {
        "collection": {
            "name": f'collection_name {random_value()}'
        },
        "type": aggregation_type,
        "configuration": {
            "on": [
                random_enum(AggregationTrigger).id
            ],
            "target_field": f'field_{random_value()}'
        },
        "name": f'name_{random_value()}',
        "aggregation": {
            "name": f'aggregation_name_{random_value()}',
            "type": aggregation_type,
            "payload": {

            }
        }
    }


def random_value():
    return random.randrange(1, 30)
