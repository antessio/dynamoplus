import unittest
import decimal
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.repository.repositories import Repository
from dynamoplus.models.indexes.indexes import Query, Index
from dynamoplus.repository.models import QueryResult, Model
from dynamoplus.service.dynamoplus import DynamoPlusService

from moto import mock_dynamodb2
import boto3
import os

@mock_dynamodb2
class TestDynamoPlusService(unittest.TestCase):

    @mock_dynamodb2
    def setUp(self):
        os.environ["DYNAMODB_TABLE"]="example_1"
        self.dynamodb = boto3.resource("dynamodb")
        # self.table=self.dynamodb.create_table(TableName="example_1",
        #     KeySchema=[
        #         {'AttributeName': 'pk','KeyType': 'HASH'},
        #         {'AttributeName': 'sk','KeyType': 'RANGE'}
        #     ],
        #     AttributeDefinitions=[
        #             {'AttributeName':'pk','AttributeType':'S'},
        #             {'AttributeName':'sk','AttributeType':'S'},
        #             {'AttributeName':'data','AttributeType':'S'}
        #     ],
        #     GlobalSecondaryIndexes=[
        #         {
        #             'IndexName': 'sk-data-index',
        #             'KeySchema': [{'AttributeName': 'sk','KeyType': 'HASH'},{'AttributeName': 'data','KeyType': 'RANGE'}],
        #             "Projection":{"ProjectionType": "ALL"}
        #         }
        #     ]
        # )
        # print("Table status:", self.table.table_status)
    
    @mock_dynamodb2
    def tearDown(self):
        #print("Table status tear down:", self.table.table_status)
        del os.environ["DYNAMODB_TABLE"]
    @mock_dynamodb2
    def test_getRepositoryByDocumentType_inSystem(self):
        dynamoPlusService = DynamoPlusService("document_type#id#ordering","document_type#name")
        result = dynamoPlusService.getRepositoryByDocumentType("document_type")
        self.assertEqual(result.documentTypeConfiguration.entityName, "document_type")
        self.assertEqual(result.documentTypeConfiguration.idKey, "id")
        self.assertEqual(result.documentTypeConfiguration.orderingKey, "ordering")
    @mock_dynamodb2
    def test_getRepositoryByDocumentType_inDB(self):
        document = {"id": "1", "name": "example", "creation_date_time":"1234114","idKey":"id","orderingKey":"ordering"}
        self.table = self.getMockTable()
        # self.table = self.dynamodb.Table('example_1')
        self.table.put_item(Item={"pk":"document_type#1","sk":"document_type","data":"1", **document})
        self.table.put_item(Item={"pk":"document_type#1","sk":"document_type#name","data":"example", **document})        
        dynamoPlusService = DynamoPlusService("document_type#id#ordering","document_type#name")
        result = dynamoPlusService.getRepositoryByDocumentType("example")
        self.assertEqual(result.documentTypeConfiguration.entityName, "example")
        self.assertEqual(result.documentTypeConfiguration.idKey, "id")
        self.assertEqual(result.documentTypeConfiguration.orderingKey, "ordering")

    def getMockTable(self):
        table = self.dynamodb.create_table(TableName="example_1",
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
        print("Table status:", table.table_status)
        return table