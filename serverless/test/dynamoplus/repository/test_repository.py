import unittest
from typing import *
from dynamoplus.repository.models import Model,IndexModel
from dynamoplus.models.indexes.indexes import Query,Index
from dynamoplus.repository.repositories import DomainRepository
from dynamoplus.models.system.collection.collection import Collection
from moto import mock_dynamodb2
import boto3
import os

from boto3.dynamodb.conditions import Key, Attr

@mock_dynamodb2
class TestDomainRepository(unittest.TestCase):
    @mock_dynamodb2
    def setUp(self):
        os.environ["TEST_FLAG"]="true"
        os.environ["DYNAMODB_DOMAIN_TABLE"]="example"
        self.dynamodb = boto3.resource("dynamodb")
        self.dynamodb.create_table(TableName="example",
            KeySchema=[
                {'AttributeName': 'pk','KeyType': 'HASH'},
                {'AttributeName': 'sk','KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                    {'AttributeName':'pk','AttributeType':'S'},
                    {'AttributeName':'sk','AttributeType':'S'},
                    {'AttributeName':'data','AttributeType':'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'sk-data-index',
                    'KeySchema': [{'AttributeName': 'sk','KeyType': 'HASH'},{'AttributeName': 'data','KeyType': 'RANGE'}],
                    "Projection":{"ProjectionType": "ALL"}
                }
            ]
        )
        self.collection = Collection("example", "id","ordering")
        self.repository = DomainRepository(self.collection)
        self.table = self.dynamodb.Table('example')
    def tearDown(self):
        self.table.delete()
        del os.environ["DYNAMODB_DOMAIN_TABLE"]
    def test_create(self):
        document = {"id": "randomUid","ordering":"1"}
        result = self.repository.create(document)
        self.assertIsNotNone(result)
        self.assertIn("pk",result.toDynamoDbItem())
        self.assertIn("sk",result.toDynamoDbItem())
        self.assertIn("data",result.toDynamoDbItem())
    def test_update(self):
        document = {"id": "1234", "attribute1":"value1"}
        self.table.put_item(Item={"pk":"example#1234","sk":"example","data":"1234",**document})
        document["attribute1"]="value2"
        result = self.repository.update(document)
        self.assertIsNotNone(result)
        self.assertIn("pk",result.toDynamoDbItem())
        self.assertIn("sk",result.toDynamoDbItem())
        self.assertIn("data",result.toDynamoDbItem())
        self.assertEqual(result.toDynamoDbItem()["attribute1"],"value2")
    def test_delete(self):
        document = {"id": "1234", "attribute1":"value1"}
        self.table.put_item(Item={"pk":"example#1234","sk":"example","data":"1234",**document})
        self.repository.delete("1234")
    def test_get(self):
        document = {"id": "1234", "attribute1":"value1"}
        self.table.put_item(Item={"pk":"example#1234","sk":"example","data":"1234",**document})
        result = self.repository.get("1234")
        self.assertIsNotNone(result)
        self.assertIn("pk",result.toDynamoDbItem())
        self.assertIn("sk",result.toDynamoDbItem())
        self.assertIn("data",result.toDynamoDbItem())
        self.assertEqual(result.toDynamoDbItem()["pk"],"example#1234")
        self.assertEqual(result.toDynamoDbItem()["sk"],"example")
        self.assertEqual(result.toDynamoDbItem()["data"],"1234")
    def test_query(self):
        for i in range(1,10):
            document = {"id": str(i), "attribute1": str(i%2), "attribute2":"value_"+str(i)}
            self.table.put_item(Item={"pk":"example#"+str(i),"sk":"example","data":str(i),**document})
            self.table.put_item(Item={"pk":"example#"+str(i),"sk":"example#attribute1","data":str(i%2),**document})
        index = Index("example",["attribute1"])
        query = Query({"attribute1":"1"},index)
        result = self.repository.find(query)
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data),5)
