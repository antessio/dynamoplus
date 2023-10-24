from __future__ import annotations
import abc
import json
import logging
import os
from _decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
from typing import *

import boto3
from boto3.dynamodb.conditions import Key, ComparisonCondition

from dynamoplus.utils.utils import sanitize

FIELD_SEPARATOR = "#"

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.DEBUG)
connection = None
try:
    if os.environ["STAGE"] and "local" == os.environ["STAGE"]:
        host = os.environ["DYNAMODB_HOST"]
        port = os.environ["DYNAMODB_PORT"]
        logging.info("using dynamolocal")
        endpoint_url = "{}:{}/".format(host, port) if host else "http://localhost:8000/"
        connection = boto3.resource('dynamodb', endpoint_url=endpoint_url)
    elif not os.environ["TEST_FLAG"]:
        connection = boto3.resource('dynamodb')
except:
    logger.info("Unable to instantiate")


@dataclass(frozen=True)
class Counter:
    field_name: str
    count: Decimal
    is_increment: bool = True


@dataclass(frozen=True)
class AtomicIncrement:
    pk: str
    sk: str
    counters: List[Counter]


class DynamoDBModel:

    @staticmethod
    def from_dynamo_db_item(dynamo_db_item: dict):
        pk = dynamo_db_item["pk"] if "pk" in dynamo_db_item else None
        sk = dynamo_db_item["sk"]
        data = dynamo_db_item["data"] if "data" in dynamo_db_item else None

        document = None
        if "document" in dynamo_db_item and dynamo_db_item["document"] is not None:
            if isinstance(dynamo_db_item["document"], dict):
                document = dynamo_db_item["document"]
            else:
                document = json.loads(dynamo_db_item["document"],
                                      parse_float=Decimal) if "document" in dynamo_db_item else {}

        return DynamoDBModel(pk, sk, data, document)

    def __init__(self, pk: str, sk: str, data: str, document: dict):
        self.pk = pk
        self.sk = sk
        self.data = data
        self.document = document

    def __members(self):
        return self.pk, self.sk, self.data, self.document

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__members() == other.__members()
        else:
            return False

    def __str__(self):
        return "{" + ",".join(map(lambda x: x.__str__(), self.__members())) + "}"

    def __hash__(self):
        return hash(self.__members())

    def to_dynamo_db_item(self):
        return {
            "document": self.document,
            "pk": self.pk,
            "sk": self.sk,
            "data": self.data
        }


class QueryResult(object):
    def __init__(self, data: List["DynamoDBModel"], last_evaluated_key: DynamoDBKey = None):
        self.data = data
        self.lastEvaluatedKey = last_evaluated_key

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return ".".join(map(lambda model: model.document, self.data))

    def __eq__(self, o: object) -> bool:
        if isinstance(o, QueryResult):
            if len(o.data) == len(self.data):
                # return self.data == o.data
                return True
        else:
            return super().__eq__(o)


@dataclass(frozen=True)
class DynamoDBKey:
    partition_key: str
    sort_key: str
    data: str

    @staticmethod
    def from_dynamo_db_item(dynamo_db_item: dict) -> DynamoDBKey:
        partition_key = dynamo_db_item["pk"] if "pk" in dynamo_db_item else None
        sort_key = dynamo_db_item["sk"]
        data = dynamo_db_item["data"] if "data" in dynamo_db_item else None
        return DynamoDBKey(partition_key, sort_key, data)


class DynamoDBQueryType(Enum):
    BEGINS_WITH = "BEGINS_WITH"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"
    ALL = "ALL",
    BETWEEN = "BETWEEN"

    @staticmethod
    def value_of(value) -> Enum:
        for m, mm in DynamoDBQueryType.__members__.items():
            if m == value.upper():
                return mm


@dataclass
class DynamoDBQuery:
    partition_key_name: str = field(init=False, default='pk')
    sort_key_name: str = field(init=False, default='sk')
    _condition: ComparisonCondition = field(init=False, default=None)

    def begins_with(self, partition_key, sort_key: str) -> DynamoDBQuery:
        self._condition = Key(self.partition_key_name).eq(partition_key) & Key(self.sort_key_name).begins_with(sort_key)
        return self

    def eq(self, partition_key, sort_key: str) -> DynamoDBQuery:
        self._condition = Key(self.partition_key_name).eq(partition_key) & Key(self.sort_key_name).eq(sort_key)
        return self

    def gt(self, partition_key, sort_key: str) -> DynamoDBQuery:
        self._condition = Key(self.partition_key_name).eq(partition_key) & Key(self.sort_key_name).gt(sort_key)
        return self

    def gte(self, partition_key, sort_key: str) -> DynamoDBQuery:
        self._condition = Key(self.partition_key_name).eq(partition_key) & Key(self.sort_key_name).gte(sort_key)
        return self

    def lt(self, partition_key, sort_key: str) -> DynamoDBQuery:
        self._condition = Key(self.partition_key_name).eq(partition_key) & Key(self.sort_key_name).lt(sort_key)
        return self

    def lte(self, partition_key, sort_key: str) -> DynamoDBQuery:
        self._condition = Key(self.partition_key_name).eq(partition_key) & Key(self.sort_key_name).lte(sort_key)
        return self

    def between(self, partition_key, sort_key_from: str, sort_key_to: str) -> DynamoDBQuery:
        self._condition = Key(self.partition_key_name).eq(partition_key) & Key(self.sort_key_name).between(
            sort_key_from, sort_key_to)
        return self

    def all(self, partition_key):
        self._condition = Key(self.partition_key_name).eq(partition_key)
        return self

    def get_query_condition_expression(self):
        return self._condition


@dataclass(frozen=True)
class DynamoDBItem:
    partition_key: str
    sort_key: str
    data: str
    document: dict

    @staticmethod
    def from_dict(dynamo_db_item: dict):
        pk = dynamo_db_item["pk"] if "pk" in dynamo_db_item else None
        sk = dynamo_db_item["sk"]
        data = dynamo_db_item["data"] if "data" in dynamo_db_item else None
        document = None
        if "document" in dynamo_db_item:
            if isinstance(dynamo_db_item["document"], dict):
                document = dynamo_db_item["document"]
            else:
                document = json.loads(dynamo_db_item["document"],
                                      parse_float=Decimal) if "document" in dynamo_db_item else {}

        return DynamoDBItem(pk, sk, data, document)

    def to_dict(self):
        return {
            "document": self.document,
            "pk": self.partition_key,
            "sk": self.sort_key,
            "data": self.data
        }


@dataclass(frozen=True)
class DynamoDBQueryResult:
    data: List[DynamoDBItem]
    last_evaluated_key: DynamoDBKey


@dataclass
class GSIDynamoDBQuery(DynamoDBQuery):
    partition_key_name: str = field(init=False, default='sk')
    sort_key_name: str = field(init=False, default='data')


class DynamoDBDAO:

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self.dynamo_db = connection if connection is not None else boto3.resource('dynamodb')
        self.table = self.dynamo_db.Table(self.table_name)

    def cleanup_table(self):
        table = self.dynamo_db.Table(self.table_name)
        scan = table.scan()
        with table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(
                    Key={
                        'pk': each['pk'],
                        'sk': each['sk']
                    }
                )

    def create_table(self):
        table = self.dynamo_db.create_table(TableName=self.table_name,
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
                                                    "Projection": {"ProjectionType": "ALL"},
                                                    "ProvisionedThroughput": {'WriteCapacityUnits': 1,
                                                                              'ReadCapacityUnits': 1}
                                                }
                                            ],
                                            ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
                                            )
        logging.info("Table status: {}".format(table.table_status))

    def create(self, model: DynamoDBModel):
        dynamoDbItem = model.to_dynamo_db_item()
        response = self.table.put_item(Item=sanitize(dynamoDbItem))
        logger.info("Response from put item operation is " + response.__str__())
        return model

    def get(self, partition_key: str, sort_key: str):
        result = self.table.get_item(
            Key={
                'pk': partition_key,
                'sk': sort_key
            })
        logging.info("Result = {}".format(result))
        return DynamoDBModel.from_dynamo_db_item(result[u'Item']) if 'Item' in result else None

    def increment_counter(self, atomic_increment: AtomicIncrement):

        # only updates attributes in the id_key or pk or sk

        # update_expression = "SET document.#field_name1 = document.#field_name1 + :increase1, document.#field_name2 = document.#field_name2 + :increase2"
        update_expression = "SET "
        expression_attribute_values = {}
        expression_attribute_names = {}

        for counter in atomic_increment.counters:
            counter_name = counter.field_name
            counter_value = counter.count
            # Add the counter name to the UpdateExpression
            update_expression += f"#data_name.#increment_name = #data_name.#increment_name + :increment, "

            # Add the counter name and value to ExpressionAttributeValues
            expression_attribute_values[f":increment"] = counter_value
            # expression_attribute_values[f"#increment_name"] = counter_name
            expression_attribute_names[f"#increment_name"] = 'count'
            expression_attribute_names[f'#data_name'] = 'document'
        update_expression = update_expression.rstrip(', ')

        # update_expression = 'SET {}'.format(','.join(
        #     f'{k.field_name}= {k.field_name} {"+" if k.is_increment else "-"} :_{k.field_name}_' for k
        #     in atomic_increment.counters))
        # expression_attribute_values = {f':{k.field_name}': k.count for k in atomic_increment.counters}
        # expression_attribute_names = {f'#{k.field_name}': k.field_name for k in atomic_increment.counters}

        response = self.table.update_item(
            Key={
                'pk': atomic_increment.pk,
                'sk': atomic_increment.sk
            },
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW"
        )
        logger.info("Response from update operation is " + response.__str__())
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            logger.error("The status is {}".format(response['ResponseMetadata']['HTTPStatusCode']))
            return False

    def update(self, model: DynamoDBModel):
        dynamo_db_item = model.to_dynamo_db_item()
        if dynamo_db_item.keys():
            # only updates attributes in the id_key or pk or sk
            logger.info("updating {} ".format(dynamo_db_item))

            response = self.table.put_item(Item=sanitize(dynamo_db_item))

            logger.info("Response from update operation is " + response.__str__())
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return model
            else:
                logger.error("The status is {}".format(response['ResponseMetadata']['HTTPStatusCode']))
                return None
        else:
            raise Exception("dynamo db item empty ")

    def delete(self, partition_key: str, sort_key: str):
        response = self.table.delete_item(
            Key={
                'pk': partition_key,
                'sk': sort_key
            })
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logger.error("The status is {}".format(response['ResponseMetadata']['HTTPStatusCode']))
            raise Exception("Error code {}".format(response['ResponseMetadata']['HTTPStatusCode']))

    def query(self, query: DynamoDBQuery, limit: int, start_from: DynamoDBKey = None) -> QueryResult:
        exclusive_start_key = None
        if start_from:
            exclusive_start_key = {"pk": start_from, "sk": start_from.sort_key, "data": start_from.data}
        dynamo_query = {
            'KeyConditionExpression': query.get_query_condition_expression(),
            'Limit': limit,
            'ScanIndexForward': False, 'ExclusiveStartKey': exclusive_start_key
        }
        if isinstance(query, GSIDynamoDBQuery):
            dynamo_query['IndexName'] = "sk-data-index"

        response = self.table.query(
            **{k: v for k, v in dynamo_query.items() if v is not None}
        )
        logger.info("Response from dynamo db {}".format(str(response)))
        last_key = None
        if 'LastEvaluatedKey' in response:
            last_key = response['LastEvaluatedKey']
            logging.debug("last key = {}", last_key)
        return QueryResult(list(map(lambda i: DynamoDBModel.from_dynamo_db_item(i), response[u'Items'])),
                           DynamoDBKey.from_dynamo_db_item(last_key) if last_key else None)



    def __query_gsi(self, key, limit, last_key):
        start_from = None
        if last_key:
            start_from = {"pk": last_key.pk, "sk": last_key.sk, "data": last_key.data}
        dynamo_query = {'IndexName': "sk-data-index",
                        'KeyConditionExpression': key,
                        'Limit': limit,
                        'ScanIndexForward': False, 'ExclusiveStartKey': start_from}
        response = self.table.query(
            **{k: v for k, v in dynamo_query.items() if v is not None}
        )
        logger.info("Response from dynamo db {}".format(str(response)))
        last_key = None
        if 'LastEvaluatedKey' in response:
            last_key = response['LastEvaluatedKey']
            logging.debug("last key = {}", last_key)
        return QueryResult(list(map(lambda i: DynamoDBModel.from_dynamo_db_item(i), response[u'Items'])),
                           DynamoDBKey.from_dynamo_db_item(last_key) if last_key else None)


class QueryRepository:

    def __init__(self, table_name: str):
        self.tableName = table_name
        self.dynamoDB = connection if connection is not None else boto3.resource('dynamodb')
        self.table = self.dynamoDB.Table(self.tableName)

    def query_begins_with(self, sk: str, data: str, last_key: DynamoDBModel = None, limit: int = 20):
        key = Key('sk').eq(sk) & Key('data').begins_with(data)
        logger.info(
            "The key that will be used is sk={} begins with data={}".format(sk, data))
        return self.__query_gsi(key, limit, last_key)

    def query_gt(self, sk: str, data: str, limit: int = 20, last_key: DynamoDBModel = None):
        key = Key('sk').eq(sk) & Key('data').gt(data)
        logger.info(
            "The key that will be used is sk={} begins with data={}".format(sk, data))
        return self.__query_gsi(key, limit, last_key)

    def query_lt(self, sk: str, data: str, limit: int = 20, last_key: DynamoDBModel = None):
        key = Key('sk').eq(sk) & Key('data').lt(data)
        logger.info(
            "The key that will be used is sk={} begins with data={}".format(sk, data))
        return self.__query_gsi(key, limit, last_key)

    def query_gt(self, sk: str, data: str, limit: int = 20, last_key: DynamoDBModel = None):
        key = Key('sk').eq(sk) & Key('data').gte(data)
        logger.info(
            "The key that will be used is sk={} begins with data={}".format(sk, data))
        return self.__query_gsi(key, limit, last_key)

    def query_lt(self, sk: str, data: str, limit: int = 20, last_key: DynamoDBModel = None):
        key = Key('sk').eq(sk) & Key('data').lte(data)
        logger.info(
            "The key that will be used is sk={} begins with data={}".format(sk, data))
        return self.__query_gsi(key, limit, last_key)

    def query_all(self, sk: str, last_key: DynamoDBModel = None, limit: int = 20):
        key = Key('sk').eq(sk)
        logger.info("The key that will be used is sk={} with no data".format(sk))
        return self.__query_gsi(key, limit, last_key)

    def query_range(self, sk: str, from_data: str, to_data: str, limit: int = 20, last_key: DynamoDBModel = None):
        v_1 = from_data
        v_2 = to_data
        key = Key('sk').eq(sk) & Key('data').between(v_1, v_2)
        logger.info(
            "the key that will be used is sk={} and data between {} and {}".format(sk, v_1, v_2))

        return self.__query_gsi(key, limit, last_key)

    def __query_gsi(self, key, limit, last_key):
        start_from = None
        if last_key:
            start_from = {"pk": last_key.pk, "sk": last_key.sk, "data": last_key.data}
        dynamo_query = dict(
            IndexName="sk-data-index",
            KeyConditionExpression=key,
            Limit=limit,
            ScanIndexForward=False,
            ExclusiveStartKey=start_from
        )
        response = self.table.query(
            **{k: v for k, v in dynamo_query.items() if v is not None}
        )
        logger.info("Response from dynamo db {}".format(str(response)))
        last_key = None
        if 'LastEvaluatedKey' in response:
            last_key = response['LastEvaluatedKey']
            logging.debug("last key = {}", last_key)
        return QueryResult(list(map(lambda i: DynamoDBModel.from_dynamo_db_item(i), response[u'Items'])),
                           DynamoDBKey.from_dynamo_db_item(last_key) if last_key else None)


def get_table_name(is_system: bool):
    return os.environ['DYNAMODB_DOMAIN_TABLE'] if not is_system else os.environ['DYNAMODB_SYSTEM_TABLE']


def is_local_environment():
    return "STAGE" in os.environ and "local" == os.environ["STAGE"] and (
            "TEST_FLAG" not in os.environ or "true" != os.environ["TEST_FLAG"])


def cleanup_tables():
    if not is_local_environment():
        return None
    try:
        tableName = os.environ['DYNAMODB_DOMAIN_TABLE']
        dynamo_db = connection if connection is not None else boto3.resource('dynamodb')
        table = dynamo_db.Table(tableName)
        scan = table.scan()
        with table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(
                    Key={
                        'pk': each['pk'],
                        'sk': each['sk']
                    }
                )
    except Exception as e:
        logging.error("Unable to cleanup the table {} ".format("domain"), e)
    try:
        tableName = os.environ['DYNAMODB_SYSTEM_TABLE']
        system_table = dynamo_db.Table(tableName)
        scan = system_table.scan()
        with system_table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(
                    Key={
                        'pk': each['pk'],
                        'sk': each['sk']
                    }
                )
    except Exception as e:
        logging.error("Unable to cleanup the table {} ".format("system"), e)


def create_tables():
    logger.info("create tables")
    dynamo_db = connection if connection is not None else boto3.resource('dynamodb')
    try:
        domain_table = dynamo_db.create_table(TableName=os.environ['DYNAMODB_DOMAIN_TABLE'],
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
                                                      "Projection": {"ProjectionType": "ALL"},
                                                      "ProvisionedThroughput": {'WriteCapacityUnits': 1,
                                                                                'ReadCapacityUnits': 1}
                                                  }
                                              ],
                                              ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
                                              )
        logging.info("Table status: {}".format(domain_table.table_status))
    except Exception as e:
        logging.error("Unable to create the table {} ".format("domain"), e)
    try:
        system_table = dynamo_db.create_table(TableName=os.environ['DYNAMODB_SYSTEM_TABLE'],
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
                                                      "Projection": {"ProjectionType": "ALL"},
                                                      "ProvisionedThroughput": {'WriteCapacityUnits': 1,
                                                                                'ReadCapacityUnits': 1}
                                                  }
                                              ],
                                              ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
                                              )
        logging.info("Table status: {}".format(system_table.table_status))
    except Exception as e:
        logging.error("Unable to create the table {} ".format("system"), e)
