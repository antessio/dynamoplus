import unittest
import decimal
import logging
from dynamoplus.models.query.query import Index
#from dynamoplus.models.documents.documentTypes import Collection
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.repository.repositories import DynamoPlusRepository
from dynamoplus.models.query.query import Query, Index
from dynamoplus.repository.models import QueryResult, Model
from dynamoplus.service.dynamoplus import DynamoPlusService

from moto import mock_dynamodb2
import boto3
import os


logging.getLogger('boto').setLevel(logging.DEBUG)

@mock_dynamodb2
class TestDynamoPlusService(unittest.TestCase):

    @mock_dynamodb2
    def setUp(self):
        os.environ["DYNAMODB_DOMAIN_TABLE"] = "example-domain"
        os.environ["DYNAMODB_SYSTEM_TABLE"] = "example-system"
        self.dynamodb = boto3.resource("dynamodb", region_name='eu-west-1')
        os.environ["ENTITIES"] = "collection#id#creation_date_time,index#id#creation_date_time"
        os.environ["INDEXES"] = "collection#name,index#name,index#collection.name"
        self.system_table = self.getMockTable("example-system")
        self.fillSystemData()
        self.dynamoPlus = DynamoPlusService()
        #self.getMockTable("example-domain")
        #self.table = self.getMockTable("example-domain")



    def tearDown(self):
        #self.system_table.delete()
        del os.environ["DYNAMODB_DOMAIN_TABLE"]
        del os.environ["DYNAMODB_SYSTEM_TABLE"]

    # @mock_dynamodb2
    # def test_getIndexFromCollectionName(self):
    #     query = self.dynamoPlus.get_indexes_from_collecion_name("example")
    #     self.assertEqual(len(query),1)
        
    # @mock_dynamodb2
    # def test_getCollectionFromCollectionName_inSystem(self):
    #     result = dynamoPlus.getCollectionConfigurationFromCollectionName("collection")
    #     self.assertEqual(result.name, "collection")
    #     self.assertEqual(result.idKey, "name")
    #     self.assertEqual(result.orderingKey, "ordering")
    # @mock_dynamodb2
    # def test_getCollectionFromCollectionName_inDB(self):
    #     document = {"id": "1", "name": "example", "creation_date_time":"1234114","idKey":"id","orderingKey":"ordering"}
    #     self.tableDomain = self.getMockTable()
    #     self.tableDomain.put_item(Item={"pk":"collection#1","sk":"collection","data":"1", **document})
    #     self.tableDomain.put_item(Item={"pk":"collection#1","sk":"collection#name","data":"example", **document})        
    #     dynamoPlusService = DynamoPlusService()
    #     result = dynamoPlusService.getCollectionConfigurationFromCollectionName("example")
    #     self.assertEqual(result.name, "example")
    #     self.assertEqual(result.idKey, "id")
    #     self.assertEqual(result.orderingKey, "ordering")
    
    # def test_getSystemIndexConfigurationsFromDocumentType(self):
    #     self.fillSystemData()
    #     dynamoPlusService = DynamoPlusService()
    #     result = dynamoPlusService.getIndexConfigurationsByCollectionName("collection")
    #     self.assertIsNotNone(len(result),1)
    #     index=result[0]
    #     self.assertEqual(index.documentType,"collection")
    #     self.assertEqual(index.conditions,["name"])
    #     result = dynamoPlusService.getIndexConfigurationsByCollectionName("index")
    #     self.assertIsNotNone(len(result),2)
    #     index=result[0]
    #     self.assertEqual(index.documentType,"index")
    #     self.assertEqual(index.conditions,["name"])
    #     index=result[1]
    #     self.assertEqual(index.documentType,"index")
    #     self.assertEqual(index.conditions,["collection.name"])

    # def test_getCustomIndexConfigurationsFromDocumentType(self):
    #     document = {"id": "1", "name": "example", "creation_date_time":"1234114","idKey":"id","orderingKey":"ordering"}
    #     self.tableDomain = self.getMockTable()
    #     # self.tableDomain = self.dynamodb.Table('example_1')
    #     self.tableDomain.put_item(Item={"pk":"collection#1","sk":"collection","data":"1", **document})
    #     self.tableDomain.put_item(Item={"pk":"collection#1","sk":"collection#name","data":"example", **document})        
    #     self.tableDomain.put_item(Item={"pk":"index#1","sk":"index","data":"1", "name": "name__ORDER_BY__ordering", "collection":{"name":"example"}})
    #     self.tableDomain.put_item(Item={"pk":"index#1","sk":"index#name","data":"name__ORDER_BY__ordering", "name": "name__ORDER_BY__ordering", "collection":{"name":"example"}})
    #     self.tableDomain.put_item(Item={"pk":"index#1","sk":"index#collection.name","data":"example","name": "name__ORDER_BY__ordering", "collection":{"name":"example"}})
    #     dynamoPlusService = DynamoPlusService()
    #     result = dynamoPlusService.getIndexConfigurationsByCollectionName("example")
    #     self.assertIsNotNone(len(result),1)
    #     index=result[0]
    #     self.assertEqual(index.documentType,"example")
    #     self.assertEqual(index.conditions,["name"])
    #     self.assertEqual(index.orderingKey,"ordering")
    # def test_getIndexServiceByIndex(self):
    #     document = {"id": "1", "name": "example", "creation_date_time":"1234114","idKey":"id","orderingKey":"ordering"}
    #     self.tableDomain = self.getMockTable()
    #     # self.tableDomain = self.dynamodb.Table('example_1')
    #     self.tableDomain.put_item(Item={"pk":"collection#1","sk":"collection","data":"1", **document})
    #     self.tableDomain.put_item(Item={"pk":"collection#1","sk":"collection#name","data":"example", **document})        
    #     self.tableDomain.put_item(Item={"pk":"index#1","sk":"index","data":"1", "name": "name__ORDER_BY__ordering", "collection":{"name":"example"}})
    #     self.tableDomain.put_item(Item={"pk":"index#1","sk":"index#name","data":"name__ORDER_BY__ordering", "name": "name__ORDER_BY__ordering", "collection":{"name":"example"}})
    #     self.tableDomain.put_item(Item={"pk":"index#1","sk":"index#collection.name","data":"example","name": "name__ORDER_BY__ordering", "collection":{"name":"example"}})
    #     self.tableDomain.put_item(Item={"pk":"example#1","sk":"example","data":"1","name": "value_1", "ordering":"1"})
    #     self.tableDomain.put_item(Item={"pk":"example#1","sk":"example#name","data":"value_1#1","name": "value_1", "ordering":"1"})
    #     dynamoPlusService = DynamoPlusService()
    #     indexService = dynamoPlusService.getIndexServiceByIndex("example","name__ORDER_BY__ordering")
    #     self.assertIsNotNone(indexService)
    #     result,lastKey = indexService.findDocuments({"name":"value_1"})
    #     self.assertIsNotNone(result)
    #     self.assertEqual(len(result),1)

    def fillSystemData(self):
        system_table = self.dynamodb.Table('example-system')
        system_table.put_item(Item={"pk": "collection#example", "sk": "collection", "data": "example", "document": "{\"name\":\"example\",\"fields\": [{\"field1\": \"string\"}, {\"field2.field21\": \"string\"}]}"})
        system_table.put_item(Item={"pk": "index#collection.name", "sk": "index#collection.name", "data": "example", "document": "{\"name\":\"collection.name\",\"collection\":{\"name\":\"example\"},\"fields\": [{\"field1\": \"string\"}, {\"field2.field21\": \"string\"}]}"})

    def getMockTable(self, tableName):
        table = self.dynamodb.create_table(TableName=tableName,
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
        print("Table status:", table.table_status)
        return table