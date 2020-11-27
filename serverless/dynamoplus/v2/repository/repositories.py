from _decimal import Decimal
from typing import *
import abc
import logging
import json
from dynamoplus.utils.utils import sanitize
from boto3.dynamodb.conditions import Key, Attr
import boto3
import os
from dynamoplus.utils.utils import auto_str
from dynamoplus.utils.decimalencoder import DecimalEncoder

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


@auto_str
class Model(object):
    @staticmethod
    def from_dynamo_db_item(dynamo_db_item:dict):
        pk = dynamo_db_item["pk"] if "pk" in dynamo_db_item else None
        sk = dynamo_db_item["sk"]
        data = dynamo_db_item["data"]
        document = json.loads(dynamo_db_item["document"], parse_float=Decimal)
        return Model(pk,sk,data,document)

    def __init__(self, pk: str, sk: str, data: str, document: dict):
        self.pk = pk
        self.sk = sk
        self.data = data
        self.document = document

    def to_dynamo_db_item(self):
        return {
            "document": json.dumps(self.document, cls=DecimalEncoder),
            "pk": self.pk,
            "sk": self.sk,
            "data": self.data
        }

class QueryResult(object):
    def __init__(self, data: List["Model"], last_evaluated_key: dict = None):
        self.data = data
        self.lastEvaluatedKey = last_evaluated_key

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return ".".join(map(lambda model: model.document, self.data))

    def __eq__(self, o: object) -> bool:
        if o is isinstance(QueryResult):
            if len(o.data) == len(self.data):
                # return self.data == o.data
                return True
        else:
            return super().__eq__(o)


class RepositoryInterface(abc.ABC):

    @abc.abstractmethod
    def create(self, model: Model):
        pass

    @abc.abstractmethod
    def get(self, partition_key: str, sort_key: str):
        pass

    @abc.abstractmethod
    def update(self, model: Model):
        pass

    @abc.abstractmethod
    def delete(self, partition_key: str, sort_key: str):
        pass


class Repository(RepositoryInterface):

    def __init__(self, table_name: str) -> None:
        self.tableName = table_name
        self.dynamoDB = connection if connection is not None else boto3.resource('dynamodb')
        self.table = self.dynamoDB.Table(self.tableName)

    def create(self, model: Model):
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
        return Model.from_dynamo_db_item(result[u'Item']) if 'Item' in result else None


    def update(self, model: Model):
        dynamo_db_item = model.to_dynamo_db_item()
        if dynamo_db_item.keys():
            # only updates attributes in the id_key or pk or sk
            logger.info("updating {} ".format(dynamo_db_item))

            update_expression = "SET document=:document, data=:data"

            expression_attributes_values = {
                ":document": model.document,
                ":data": model.data
            }
            response = self.table.update_item(
                Key={
                    'pk': model.pk(),
                    'sk': model.sk()
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attributes_values,
                ReturnValues="UPDATED_NEW"
            )
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


class QueryRepository:

    def __init__(self, table_name: str):
        self.tableName = table_name
        self.dynamoDB = connection if connection is not None else boto3.resource('dynamodb')
        self.table = self.dynamoDB.Table(self.tableName)

    def query_begins_with(self,sk: str, data:str, last_key: Model = None, limit: int = 20):
        key = Key('sk').eq(sk) & Key('data').begins_with(data)
        logger.info(
            "The key that will be used is sk={} begins with data={}".format(sk, data))
        return self.__query_gsi(key, limit, last_key)

    def query_gt(self,sk: str, data:str, limit: int = 20, last_key: Model = None):
        key = Key('sk').eq(sk) & Key('data').gt(data)
        logger.info(
            "The key that will be used is sk={} begins with data={}".format(sk, data))
        return self.__query_gsi(key, limit, last_key)

    def query_lt(self,sk: str, data:str, limit: int = 20, last_key: Model = None):
        key = Key('sk').eq(sk) & Key('data').lt(data)
        logger.info(
            "The key that will be used is sk={} begins with data={}".format(sk, data))
        return self.__query_gsi(key, limit, last_key)

    def query_gt(self, sk: str, data: str, limit: int = 20, last_key: Model = None):
        key = Key('sk').eq(sk) & Key('data').gte(data)
        logger.info(
            "The key that will be used is sk={} begins with data={}".format(sk, data))
        return self.__query_gsi(key, limit, last_key)

    def query_lt(self, sk: str, data: str, limit: int = 20, last_key: Model = None):
        key = Key('sk').eq(sk) & Key('data').lte(data)
        logger.info(
            "The key that will be used is sk={} begins with data={}".format(sk, data))
        return self.__query_gsi(key, limit, last_key)


    def query_all(self,sk: str, last_key: Model = None,limit: int = 20):
        key = Key('sk').eq(sk)
        logger.info("The key that will be used is sk={} with no data".format(sk))
        return self.__query_gsi(key, limit, last_key)

    def query_range(self, sk: str, from_data: str, to_data: str, limit: int = 20, last_key: Model = None):
        v_1 = from_data
        v_2 = to_data
        key = Key('sk').eq(sk) & Key('data').between(v_1, v_2)
        logger.info(
            "the key that will be used is sk={} and data between {} and {}".format(sk, v_1, v_2))

        return self.__query_gsi(key, limit, last_key)

    def __query_gsi(self, key, limit, last_key):
        start_from = None
        if last_key:
            start_from = {"pk": last_key.pk(), "sk": last_key.sk(), "data": last_key.data()}
        dynamo_query = dict(
            IndexName="sk-data-index",
            KeyConditionExpression=key,
            Limit=limit,
            ExclusiveStartKey=start_from
        )
        response = self.table.query(
            **{k: v for k, v in dynamo_query.items() if v is not None}
        )
        logger.info("Response from dynamo db {}".format(str(response)))
        last_key = None
        if 'LastEvaluatedKey' in response:
            last_key = response['LastEvaluatedKey']["pk"].replace(self.collection.name + "#", "")
            logging.debug("last key = {}", last_key)
        return QueryResult(list(map(lambda i: Model.from_dynamo_db_item(i), response[u'Items'])),
                           last_key)


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
