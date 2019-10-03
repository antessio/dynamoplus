import unittest
from typing import *
from dynamoplus.repository.repositories import Model
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.repository.repositories import Repository
from moto import mock_dynamodb2
import boto3
import os

@mock_dynamodb2
class TestRepository(unittest.TestCase):
    @mock_dynamodb2
    def setUp(self):
        os.environ["DYNAMODB_TABLE"]="example"
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
        self.documentTypeConfiguration = DocumentTypeConfiguration("example", "id","ordering")
        self.repository = Repository(self.documentTypeConfiguration)
        self.table = self.dynamodb.Table('example')
    def tearDown(self):
        self.table.delete()
        del os.environ["DYNAMODB_TABLE"]
    def test_create(self):
        document = {"id": "randomUid","ordering":"1"}
        result = self.repository.create(document)
        self.assertIsNotNone(result)
        self.assertIn("pk",result)
        self.assertIn("sk",result)
        self.assertIn("data",result)
    def test_update(self):
        document = {"id": "1234", "attribute1":"value1"}
        self.table.put_item(Item={"pk":"example#1234","sk":"example","data":"1234",**document})
        document["attribute1"]="value2"
        result = self.repository.update(document)
        self.assertIsNotNone(result)
        self.assertIn("pk",result)
        self.assertIn("sk",result)
        self.assertIn("data",result)
        self.assertEqual(result["attribute1"],"value2")
    def test_delete(self):
        document = {"id": "1234", "attribute1":"value1"}
        self.table.put_item(Item={"pk":"example#1234","sk":"example","data":"1234",**document})
        self.repository.delete("1234")
    def test_get(self):
        document = {"id": "1234", "attribute1":"value1"}
        self.table.put_item(Item={"pk":"example#1234","sk":"example","data":"1234",**document})
        result = self.repository.get("1234")
        self.assertIsNotNone(result)
        self.assertIn("pk",result)
        self.assertIn("sk",result)
        self.assertIn("data",result)
        self.assertEqual(result["pk"],"example#1234")
        self.assertEqual(result["sk"],"example")
        self.assertEqual(result["data"],"1234")
