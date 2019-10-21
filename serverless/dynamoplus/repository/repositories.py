from typing import *
import logging
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.indexes.indexes import Index, Query
from dynamoplus.repository.models import Model,IndexModel, QueryResult
from dynamoplus.utils.utils import convertToString, findValue, sanitize
from boto3.dynamodb.conditions import Key, Attr
import boto3
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
connection = None
try:
    if not os.environ["TEST_FLAG"]:
        connection=boto3.resource('dynamodb')
except:
    logger.info("Unable to instantiate")

class Repository(object):
    def __init__(self, documentTypeConfiguration: DocumentTypeConfiguration):
        self.documentTypeConfiguration = documentTypeConfiguration
        self.tableName = os.environ['DYNAMODB_TABLE']
        self.dynamoDB = connection if connection is not None else boto3.resource('dynamodb')
        self.table = self.dynamoDB.Table(self.tableName)
    
    def getModelFromDocument(self, document:dict):
        return Model(self.documentTypeConfiguration,document)
    def create(self, document:dict):
        model = self.getModelFromDocument(document)
        dynamoDbItem = model.toDynamoDbItem()
        response = self.table.put_item(Item=sanitize(dynamoDbItem))
        logger.info("Response from put item operation is "+response.__str__())
        return self.getModelFromDocument(dynamoDbItem)
    def get(self, id:str):
        # TODO: copy from query -> if the indexKeys is empty then get by primary key, otherwise get by global secondary index
        # it means if needed first get from index, then by primary key or, in case of index it throws a non supported operation exception
        model = self.getModelFromDocument({self.documentTypeConfiguration.idKey: id})
        result = self.table.get_item(
        Key={
            'pk': model.pk(),
            'sk': model.sk()
        })
        return self.getModelFromDocument( result[u'Item']) if 'Item' in result else None
    def update(self, document:dict):
        model = self.getModelFromDocument(document)
        dynamoDbItem = model.toDynamoDbItem()
        if dynamoDbItem.keys():
            # only updates attributes in the idKey or pk or sk
            logger.info("updating {} ".format(dynamoDbItem))
            updateExpression = "SET "+", ".join(map(lambda k: k+"= :"+k, filter(lambda k: k != self.documentTypeConfiguration.idKey and k!="pk" and k!="sk" and k!="data", dynamoDbItem.keys())))
            expressionValue = dict(
                map(lambda kv: (":"+kv[0],kv[1]), 
                filter(lambda kv: kv[0] != self.documentTypeConfiguration.idKey and kv[0]!="pk" and kv[0] !="sk" and kv[0] !="data", dynamoDbItem.items())))
            response = self.table.update_item(
                Key={
                    'pk': model.pk(),
                    'sk': model.sk()
                },
                UpdateExpression=updateExpression,
                ExpressionAttributeValues=expressionValue,
                ReturnValues="UPDATED_NEW"
            )
            logger.info("Response from update operation is "+response.__str__())
            if response['ResponseMetadata']['HTTPStatusCode']==200:
                return self.getModelFromDocument(dynamoDbItem)
            else:
                logger.error("The status is {}".format(response['ResponseMetadata']['HTTPStatusCode']))
                return None
        else:
            raise Exception("dynamo db item empty ")
    def delete(self, id:str):
        model = self.getModelFromDocument({self.documentTypeConfiguration.idKey: id})
        response = self.table.delete_item(
            Key={
            'pk': model.pk(),
            'sk': model.sk()
            })
        if response['ResponseMetadata']['HTTPStatusCode']!=200:
            logger.error("The status is {}".format(response['ResponseMetadata']['HTTPStatusCode']))
            raise Exception("Error code {}".format(response['ResponseMetadata']['HTTPStatusCode']))
    def find(self, query: Query):
        logger.info(" Received query={}".format(query.__str__()))
        document = query.document
        ## TODO: use query IndexModelFactory.indexModelFromQuery
        ## if the index is a range index then data will be an array
        indexModel = IndexModel(self.documentTypeConfiguration,document,query.index)
        orderingKey = query.index.orderingKey if query.index else None
        logger.info("order by is {} ".format(orderingKey))
        limit = query.limit
        startFrom = query.startFrom
        if indexModel.data() is not None:
            if orderingKey is None:
                key=Key('sk').eq(indexModel.sk()) & Key('data').eq(indexModel.data())
                logger.info("The key that will be used is sk={} is equal data={}".format(indexModel.sk(), indexModel.data()))
            else:
                key=Key('sk').eq(indexModel.sk()) & Key('data').begins_with(indexModel.data())
                logger.info("The key that will be used is sk={} begins with data={}".format(indexModel.sk(), indexModel.data()))
        else:
            key=Key('sk').eq(indexModel.sk())
            logger.info("The key that will be used is sk={} with no data".format(indexModel.sk()))

            
        
        dynamoQuery=dict(
            IndexName="sk-data-index",
            KeyConditionExpression=key,
            Limit=limit,
            ExclusiveStartKey=startFrom
        )
        response = self.table.query(
                **{k: v for k, v in dynamoQuery.items() if v is not None}
            )
        logger.info("Response from dynamo db {}".format(str(response)))
        lastKey=None
    
        if 'LastEvaluatedKey' in response:
            lastKey=response['LastEvaluatedKey']
        return QueryResult(list(map(lambda i: Model(self.documentTypeConfiguration, i),response[u'Items'])),lastKey)


class IndexRepository(Repository):
    def __init__(self,documentTypeConfiguration: DocumentTypeConfiguration, index:Index):
        super().__init__(documentTypeConfiguration)
        self.index = index
    def getModelFromDocument(self, document:dict):
        return IndexModel(self.documentTypeConfiguration,document,self.index)