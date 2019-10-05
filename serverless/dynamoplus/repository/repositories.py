from typing import *
import logging
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.indexes.indexes import Index, Query
from dynamoplus.repository.models import Model,IndexModel, QueryResult
from dynamoplus.utils.utils import convertToString, findValue, sanitize
from boto3.dynamodb.conditions import Key, Attr
import boto3
import os
logging.basicConfig(level=logging.INFO)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)

class Repository(object):
    def __init__(self, documentTypeConfiguration: DocumentTypeConfiguration):
        self.documentTypeConfiguration = documentTypeConfiguration
        self.tableName = os.environ['DYNAMODB_TABLE']
        self.dynamoDB = boto3.resource('dynamodb')
        self.table = self.dynamoDB.Table(self.tableName)
    
    def getModelFromDocument(self, document:dict):
        return Model(self.documentTypeConfiguration,document)
    def create(self, document:dict):
        model = self.getModelFromDocument(document)
        dynamoDbItem = model.toDynamoDbItem()
        response = self.table.put_item(Item=sanitize(dynamoDbItem))
        logging.info("Response from put item operation is "+response.__str__())
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
            logging.info("updating {} ".format(dynamoDbItem))
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
            logging.info("Response from update operation is "+response.__str__())
            if response['ResponseMetadata']['HTTPStatusCode']==200:
                return self.getModelFromDocument(dynamoDbItem)
            else:
                logging.error("The status is {}".format(response['ResponseMetadata']['HTTPStatusCode']))
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
            logging.error("The status is {}".format(response['ResponseMetadata']['HTTPStatusCode']))
            raise Exception("Error code {}".format(response['ResponseMetadata']['HTTPStatusCode']))
    def find(self, query: Query):
        logging.info(" Received query={}".format(query.__str__()))
        document = query.document
        indexModel = IndexModel(self.documentTypeConfiguration,document,query.index)
        orderValue = indexModel.orderValue()
        limit = query.limit
        startFrom = query.startFrom
        if orderValue is None:
            key=Key('sk').eq(indexModel.sk()) & Key('data').eq(indexModel.data())
            logging.info("The key that will be used is sk={} data={}".format(indexModel.sk(), indexModel.data()))
        else:
            key=Key('sk').eq(indexModel.sk()) & Key('data').begins_with(indexModel.data())
            logging.info("The key that will be used is sk={} data={}".format(indexModel.sk(), indexModel.data()))
            
        
        dynamoQuery=dict(
            IndexName="sk-data-index",
            KeyConditionExpression=key,
            Limit=limit,
            ExclusiveStartKey=startFrom
        )
        response = self.table.query(
                **{k: v for k, v in dynamoQuery.items() if v is not None}
            )
        logging.info("Response from dynamo db {}".format(str(response)))
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