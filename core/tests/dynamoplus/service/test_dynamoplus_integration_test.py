import unittest
import logging
import uuid

from dynamoplus.dynamo_plus_v2 import Dynamoplus
from aws.dynamodb.dynamodb_repository import DynamoDBRepository
from moto import mock_dynamodb2
import boto3
import os

from dynamoplus.v2.repository.domain_repository import DomainEntity
from dynamoplus.v2.repository.system_repositories import AggregationConfigurationEntity, AggregationEntity, \
    ClientAuthorizationEntity, CollectionEntity, IndexEntity

logging.getLogger('boto').setLevel(logging.DEBUG)


@mock_dynamodb2
class TestDynamoPlusService(unittest.TestCase):
    dyamoplus_instance: Dynamoplus

    def setUp(self):
        domain_table_name = "domain"
        system_table_name = "system"
        os.environ["STAGE"] = "local"
        os.environ["DYNAMODB_DOMAIN_TABLE"] = domain_table_name
        os.environ["DYNAMODB_SYSTEM_TABLE"] = system_table_name
        self.dynamodb = boto3.resource("dynamodb", region_name='eu-west-1')
        os.environ["ENTITIES"] = "collection,index,client_authorization"
        self.system_table = self.getMockTable(system_table_name)
        self.domain_table = self.getMockTable(domain_table_name)
        # self.fillSystemData()
        # self.getMockTable("example-domain")
        self.dyamoplus_instance = Dynamoplus(
            DynamoDBRepository('system'),
            DynamoDBRepository('system'),
            DynamoDBRepository('system'),
            DynamoDBRepository('system'),
            DynamoDBRepository('system'),
            DynamoDBRepository('domain'))

    def tearDown(self):
        self.system_table.delete()
        self.domain_table.delete()
        del os.environ["DYNAMODB_DOMAIN_TABLE"]
        del os.environ["DYNAMODB_SYSTEM_TABLE"]
        del os.environ["STAGE"]

    def test_create_collection(self):
        collection_name = "example"
        response = self.dyamoplus_instance.create("collection", {
            "name": collection_name,
            "id_key": "id",
            "ordering": "ordering",
            "attributes": [
                {"name": "a", "type": "STRING"},
                {"name": "b", "type": "STRING"},
                {"name": "c", "type": "STRING"}
            ]
        })
        self.assertEqual(response["name"], collection_name)

    def test_create_index(self):
        collection_name = "example"
        response = self.dyamoplus_instance.create("collection", {
            "name": collection_name,
            "id_key": "id",
            "ordering": "ordering",
            "attributes": [
                {"name": "a", "type": "STRING"},
                {"name": "b", "type": "STRING"},
                {"name": "c", "type": "STRING"}
            ]
        })
        index = {
            "name": "index",
            "collection": {
                "id_key": "id",
                "name": collection_name
            },
            "conditions": ["a", "b"]
        }
        self.dyamoplus_instance.create("index", index)

    def test_create_document(self):
        collection_name = "example"
        response = self.dyamoplus_instance.create("collection", {
            "name": collection_name,
            "id_key": "name",
            "ordering": "ordering",
            "attributes": [
                {"name": "a", "type": "STRING"},
                {"name": "b", "type": "STRING"},
                {"name": "c", "type": "STRING"}
            ]
        })
        response = self.dyamoplus_instance.create(collection_name, {
            "name": "1",
            "ordering": "2",
            "attributes": {
                "a": "test1",
                "b": "test2",
                "c": "test3"
            }
        })
        self.assertEqual(response["name"], "1")
        self.assertIn("attributes", response)

    def test_create_document_nested_attributes(self):
        collection_name = "example"
        response = self.dyamoplus_instance.create("collection", {
            "name": collection_name,
            "id_key": "id",
            "ordering": "ordering",
            "attributes": [
                {"name": "a", "type": "STRING"},
                {"name": "b", "type": "STRING"},
                {"name": "c", "type": "OBJECT", "attributes": [
                    {"name": "ca", "type": "STRING"},
                    {"name": "cb", "type": "STRING"}
                ]}
            ]
        })
        response = self.dyamoplus_instance.create(collection_name, {
            "id": "1",
            "ordering": "2",
            "attributes": {
                "a": "test1",
                "b": "test2",
                "c": {"ca": "test31", "cb": "test32"}
            }
        })
        self.assertEqual(response["id"], "1")
        self.assertIn("attributes", response)

    def test_create_http_signature_client_authorization(self):
        http_signature_client_authorization = {
            "type": "http_signature",
            "client_id": str(uuid.uuid4()),
            "public_key": "test-public-key",
            "client_scopes": [
                {
                    "collection_name": "collection1",
                    "scope_type": "GET"
                }
            ]
        }
        response = self.dyamoplus_instance.create("client_authorization", http_signature_client_authorization)
        print("{}".format(response))

    def test_create_aggregation_configuration(self):
        aggregation_configuration = {'type': 'COLLECTION_COUNT',
                                     'collection': {'id_key': 'id', 'name': 'restaurant___api_key_test',
                                                    'attributes': [{'name': 'name', 'type': 'STRING'},
                                                                   {'name': 'type', 'type': 'STRING'}]},
                                     'configuration': {'on': ['DELETE', 'INSERT']}}

        response = self.dyamoplus_instance.create("aggregation_configuration", aggregation_configuration)
        print("{}".format(response))

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
        system_table.put_item(Item={"pk": "collection#example", "sk": "collection", "data": "example",
                                    "document": "{\"name\":\"example\",\"fields\": [{\"field1\": \"string\"}, {\"field2.field21\": \"string\"}]}"})
        system_table.put_item(Item={"pk": "index#collection.name", "sk": "index#collection.name", "data": "example",
                                    "document": "{\"name\":\"collection.name\",\"collection\":{\"name\":\"example\"},\"fields\": [{\"field1\": \"string\"}, {\"field2.field21\": \"string\"}]}"})

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
