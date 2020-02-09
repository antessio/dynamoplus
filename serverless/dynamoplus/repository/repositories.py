from typing import *
import abc
import logging

from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.query.query import Index, Query
from dynamoplus.repository.models import Model, IndexModel, QueryResult
from dynamoplus.utils.utils import convertToString, find_value, sanitize
from boto3.dynamodb.conditions import Key, Attr
import boto3
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.DEBUG)
connection = None
try:
    if os.environ["STAGE"] and "local" == os.environ["STAGE"]:
        host = os.environ["DYNAMODB_HOST"]
        port = os.environ["DYNAMODB_PORT"]
        endpoint_url = "{}:{}/".format(host,port) if host else "http://localhost:8000/"
        connection = boto3.resource('dynamodb', endpoint_url=endpoint_url)
    elif not os.environ["TEST_FLAG"]:
        connection = boto3.resource('dynamodb')
except:
    logger.info("Unable to instantiate")


class Repository(abc.ABC):
    @abc.abstractmethod
    def getModelFromDocument(self, document: dict):
        pass

    @abc.abstractmethod
    def create(self, document: dict):
        pass

    @abc.abstractmethod
    def get(self, id: str):
        pass

    @abc.abstractmethod
    def update(self, document: dict):
        pass

    @abc.abstractmethod
    def delete(self, id: str):
        pass

    @abc.abstractmethod
    def find(self, query: Query):
        pass


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
                                           "Projection": {"ProjectionType": "ALL"}
                                       }
                                   ]
                                   )
        logging.info("Table status: {}".format(domain_table.table_status))
    except dynamo_db.exceptions.ResourceInUseException as e:
        logging.error("Unable to create the table {} ".format("domain"),e)
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
                                              "Projection": {"ProjectionType": "ALL"}
                                          }
                                      ]
                                      )
        logging.info("Table status: {}".format(system_table.table_status))
    except dynamo_db.exceptions.ResourceInUseException as e:
        logging.error("Unable to create the table {} ".format("system"),e)

class DynamoPlusRepository(Repository):
    def __init__(self, collection: Collection, is_system=False):
        self.collection = collection
        self.tableName = os.environ['DYNAMODB_DOMAIN_TABLE'] if not is_system else os.environ['DYNAMODB_SYSTEM_TABLE']
        self.dynamoDB = connection if connection is not None else boto3.resource('dynamodb')
        self.table = self.dynamoDB.Table(self.tableName)
        self.isSystem = is_system

    def getModelFromDocument(self, document: dict):
        return Model(self.collection, document)

    def create(self, document: dict):
        model = self.getModelFromDocument(document)
        dynamoDbItem = model.to_dynamo_db_item()
        response = self.table.put_item(Item=sanitize(dynamoDbItem))
        logger.info("Response from put item operation is " + response.__str__())
        return model

    def get(self, id: str):
        # TODO: copy from query -> if the indexKeys is empty then get by primary key, otherwise get by global secondary index
        # it means if needed first get from index, then by primary key or, in case of index it throws a non supported operation exception
        model = self.getModelFromDocument({self.collection.id_key: id})
        result = self.table.get_item(
            Key={
                'pk': model.pk(),
                'sk': model.sk()
            })

        return Model.from_dynamo_db_item(result[u'Item'], self.collection) if 'Item' in result else None

    def update(self, document: dict):
        model = self.getModelFromDocument(document)
        dynamo_db_item = model.to_dynamo_db_item()
        if dynamo_db_item.keys():
            # only updates attributes in the id_key or pk or sk
            logger.info("updating {} ".format(dynamo_db_item))

            update_expression = "SET " + ", ".join(map(lambda k: "#{}=:{}".format(k,k), filter(
                lambda k: k != self.collection.id_key and k != "pk" and k != "sk",
                dynamo_db_item.keys())))
            expression_value = dict(
                map(lambda kv: (":{}".format(kv[0]), kv[1]),
                    filter(
                        lambda kv: kv[0] != self.collection.id_key and kv[0] != "pk" and kv[0] != "sk", dynamo_db_item.items())))
            expression_attribute_names = {}
            for k in dynamo_db_item.keys():
                if k != self.collection.id_key and k != "pk" and k != "sk":
                    expression_attribute_names["#{}".format(k)]=k
            response = self.table.update_item(
                Key={
                    'pk': model.pk(),
                    'sk': model.sk()
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_value,
                ExpressionAttributeNames=expression_attribute_names,
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

    def delete(self, id: str):
        model = self.getModelFromDocument({self.collection.id_key: id})
        response = self.table.delete_item(
            Key={
                'pk': model.pk(),
                'sk': model.sk()
            })
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logger.error("The status is {}".format(response['ResponseMetadata']['HTTPStatusCode']))
            raise Exception("Error code {}".format(response['ResponseMetadata']['HTTPStatusCode']))

    def find(self, query: Query):
        return None


class IndexDynamoPlusRepository(DynamoPlusRepository):
    def __init__(self, collection: Collection, index: Index, is_system=False):
        self.index = index
        super().__init__(collection, is_system)

    def getModelFromDocument(self, document: dict):
        return IndexModel(self.collection, document,self.index)

    def find(self, query: Query):
        logger.info(" Received query={}".format(query.__str__()))
        document = query.document
        index_model = IndexModel(self.collection, document, query.index)
        ordering_key = query.index.ordering_key if query.index else None
        logger.info("order by is {} ".format(ordering_key))
        limit = query.limit
        start_from = index_model.start_from(query.start_from) if query.start_from else None
        #start_from = query.start_from
        logging.info("query starts from {}".format(start_from))
        if query.index.range_condition:
            v_1, v_2 = index_model.data()
            key = Key('sk').eq(index_model.sk()) & Key('data').between(v_1,v_2)
            logger.info("the key that will be used is sk={} and data between {} and {}".format(index_model.sk(),v_1,v_2))
        else:
            if index_model.data() is not None:
                if not index_model.use_begins_with() or ordering_key is not None:
                    key = Key('sk').eq(index_model.sk()) & Key('data').begins_with(index_model.data())
                    logger.info(
                        "The key that will be used is sk={} begins with data={}".format(index_model.sk(),
                                                                                        index_model.data()))
                else:
                    key = Key('sk').eq(index_model.sk()) & Key('data').eq(index_model.data())
                    logger.info(
                        "The key that will be used is sk={} is equal data={}".format(index_model.sk(), index_model.data()))

            else:
                key = Key('sk').eq(index_model.sk())
                logger.info("The key that will be used is sk={} with no data".format(index_model.sk()))

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
            last_key = index_model.last_evaluated_key(response['LastEvaluatedKey'])
        return QueryResult(list(map(lambda i: Model.from_dynamo_db_item(i, self.collection), response[u'Items'])), last_key)
