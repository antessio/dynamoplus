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
        os.environ["TEST_FLAG"]="true"
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
    def test_getDocumentTypeConfigurationFromDocumentType_inSystem(self):
        dynamoPlusService = DynamoPlusService("collection#id#ordering","collection#name")
        result = dynamoPlusService.getDocumentTypeConfigurationFromDocumentType("collection")
        self.assertEqual(result.entityName, "collection")
        self.assertEqual(result.idKey, "id")
        self.assertEqual(result.orderingKey, "ordering")
    @mock_dynamodb2
    def test_getDocumentTypeConfigurationFromDocumentType_inDB(self):
        document = {"id": "1", "name": "example", "creation_date_time":"1234114","idKey":"id","orderingKey":"ordering"}
        self.table = self.getMockTable()
        # self.table = self.dynamodb.Table('example_1')
        self.table.put_item(Item={"pk":"collection#1","sk":"collection","data":"1", "document":{**document}})
        self.table.put_item(Item={"pk":"collection#1","sk":"collection#name","data":"example", "document":{**document}})        
        dynamoPlusService = DynamoPlusService("collection#id#ordering","collection#name")
        result = dynamoPlusService.getDocumentTypeConfigurationFromDocumentType("example")
        self.assertEqual(result.entityName, "example")
        self.assertEqual(result.idKey, "id")
        self.assertEqual(result.orderingKey, "ordering")

    def test_getSystemIndexConfigurationsFromDocumentType(self):
        dynamoPlusService = DynamoPlusService("collection#id#creation_date_time,index#id#creation_date_time","collection#name,index#name,index#collection.name")
        result = dynamoPlusService.getIndexConfigurationsByDocumentType("collection")
        self.assertIsNotNone(len(result),1)
        index=result[0]
        self.assertEqual(index.documentType,"collection")
        self.assertEqual(index.conditions,["name"])
        result = dynamoPlusService.getIndexConfigurationsByDocumentType("index")
        self.assertIsNotNone(len(result),2)
        index=result[0]
        self.assertEqual(index.documentType,"index")
        self.assertEqual(index.conditions,["name"])
        index=result[1]
        self.assertEqual(index.documentType,"index")
        self.assertEqual(index.conditions,["collection.name"])

    def test_getCustomIndexConfigurationsFromDocumentType(self):
        document = {"id": "1", "name": "example", "creation_date_time":"1234114","idKey":"id","orderingKey":"ordering"}
        self.table = self.getMockTable()
        # self.table = self.dynamodb.Table('example_1')
        self.table.put_item(Item={"pk":"collection#1","sk":"collection","data":"1", "document":{**document}})
        self.table.put_item(Item={"pk":"collection#1","sk":"collection#name","data":"example", "document":{**document}})        
        self.table.put_item(Item={"pk":"index#1","sk":"index","data":"1", "name": "name__ORDER_BY__ordering", "collection":{"name":"example"}, "document":{ "name":"name__ORDER_BY__ordering","conditions":["name"],"order_by":"ordering"}})
        self.table.put_item(Item={"pk":"index#1","sk":"index#name","data":"name__ORDER_BY__ordering", "name": "name__ORDER_BY__ordering", "collection":{"name":"example"},"document":{ "name":"name__ORDER_BY__ordering","conditions":["name"],"order_by":"ordering"}})
        self.table.put_item(Item={"pk":"index#1","sk":"index#collection.name","data":"example","name": "name__ORDER_BY__ordering", "collection":{"name":"example"},"document":{ "name":"name__ORDER_BY__ordering","conditions":["name"],"order_by":"ordering"}})
        dynamoPlusService = DynamoPlusService("collection#id#creation_date_time,index#id#creation_date_time","collection#name,index#name,index#collection.name")
        result = dynamoPlusService.getIndexConfigurationsByDocumentType("example")
        self.assertIsNotNone(len(result),1)
        index=result[0]
        self.assertEqual(index.documentType,"example")
        self.assertEqual(index.conditions,["name"])
        self.assertEqual(index.orderingKey,"ordering")
    def test_getIndexServiceByIndex(self):
        document = {"id": "1", "name": "example", "creation_date_time":"1234114","idKey":"id","orderingKey":"ordering"}
        self.table = self.getMockTable()
        # self.table = self.dynamodb.Table('example_1')
        self.table.put_item(Item={"pk":"collection#1","sk":"collection","data":"1", "document":{**document}})
        self.table.put_item(Item={"pk":"collection#1","sk":"collection#name","data":"example", "document":{**document}})        
        self.table.put_item(Item={"pk":"index#1","sk":"index","data":"1", "name": "name__ORDER_BY__ordering", "collection":{"name":"example"}, "document":{ "conditions":["name"],"order_by":"ordering"}})
        self.table.put_item(Item={"pk":"index#1","sk":"index#name","data":"name__ORDER_BY__ordering", "name": "name__ORDER_BY__ordering", "collection":{"name":"example"},"document":{ "conditions":["name"],"order_by":"ordering"}})
        self.table.put_item(Item={"pk":"index#1","sk":"index#collection.name","data":"example","name": "name__ORDER_BY__ordering", "collection":{"name":"example"},"document":{ "conditions":["name"],"order_by":"ordering"}})
        self.table.put_item(Item={"pk":"example#1","sk":"example","data":"1","name": "value_1", "ordering":"1","document":{"value_1":"1"}})
        self.table.put_item(Item={"pk":"example#1","sk":"example#name","data":"value_1#1","name": "value_1", "ordering":"1", "document":{"value_1":"1"}})
        dynamoPlusService = DynamoPlusService("collection#id#creation_date_time,index#id#creation_date_time","collection#name,index#name,index#collection.name")
        indexService = dynamoPlusService.getIndexServiceByIndex("example","name__ORDER_BY__ordering")
        self.assertIsNotNone(indexService)
        result,lastKey = indexService.findDocuments({"name":"value_1"})
        self.assertIsNotNone(result)
        self.assertEqual(len(result),1)

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