import unittest
from typing import *
from dynamoplus.models.query.query import Query, Index
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.models.system.collection.collection import Collection
from moto import mock_dynamodb2
from dynamoplus.utils.decimalencoder import DecimalEncoder
import json
import boto3
import os



@mock_dynamodb2
class TestDynamoPlusRepository(unittest.TestCase):
    @mock_dynamodb2
    def setUp(self):
        os.environ["TEST_FLAG"] = "true"
        os.environ["DYNAMODB_DOMAIN_TABLE"] = "example-domain"
        self.dynamodb = boto3.resource("dynamodb")
        self.dynamodb.create_table(TableName="example-domain",
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
        self.collection = Collection("example", "id", "ordering")
        self.repository = DynamoPlusRepository(self.collection)
        self.table = self.dynamodb.Table('example-domain')



    def tearDown(self):
        self.table.delete()
        del os.environ["DYNAMODB_DOMAIN_TABLE"]

    def test_create(self):
        document = {"id": "randomUid", "ordering": "1"}
        result = self.repository.create(document)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk)
        self.assertIsNotNone(result.sk)
        self.assertIsNotNone(result.data)
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk(), "example#randomUid")
        self.assertEqual(result.sk(), "example")
        self.assertEqual(result.data(), "randomUid#1")
        self.assertDictEqual(result.document, document)
        # self.assertEqual(result.document["attribute1"],"value2")
        # self.assertIn("pk",result.toDynamoDbItem())
        # self.assertIn("sk",result.toDynamoDbItem())
        # self.assertIn("data",result.toDynamoDbItem())

    def test_update(self):
        document = {"id": "1234", "attribute1": "value1"}
        self.table.put_item(Item={"pk": "example#1234", "sk": "example", "data": "1234", "document": json.dumps(document)})
        document["attribute1"] = "value2"
        result = self.repository.update(document)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk())
        self.assertIsNotNone(result.sk())
        self.assertIsNotNone(result.data())
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk(), "example#1234")
        self.assertEqual(result.sk(), "example")
        self.assertEqual(result.data(), "1234")
        self.assertDictEqual(result.document, document)
        self.assertEqual(result.document["attribute1"], "value2")

    def test_delete(self):
        document = {"id": "1234", "attribute1": "value1"}
        self.table.put_item(Item={"pk": "example#1234", "sk": "example", "data": "1234", **document})
        self.repository.delete("1234")

    def test_get(self):
        document = {"id": "1234", "attribute1": "value1"}
        self.table.put_item(
            Item={"pk": "example#1234", "sk": "example", "data": "1234", "document": json.dumps(document)})
        result = self.repository.get("1234")
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk())
        self.assertIsNotNone(result.sk())
        self.assertIsNotNone(result.data())
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk(), "example#1234")
        self.assertEqual(result.sk(), "example")
        self.assertEqual(result.data(), "1234")
        self.assertDictEqual(result.document, document)

    def test_query(self):
        for i in range(1, 10):
            document = {"id": str(i), "attribute1": str(i % 2), "attribute2": "value_" + str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#attribute1", "data": str(i % 2),
                                      "document": json.dumps(document)})
        index = Index("1","example", ["attribute1"])
        query = Query({"attribute1": "1"}, index)
        self.indexRepository = IndexDynamoPlusRepository(self.collection, index)
        result = self.indexRepository.find(query)
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data), 5)
        for r in result.data:
            self.assertEqual(r.document["attribute1"], '1')

    def test_query_all(self):
        for i in range(1, 10):
            document = {"id": str(i), "attribute1": str(i % 2), "attribute2": "value_" + str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#attribute1", "data": str(i % 2),
                                      "document": json.dumps(document)})
        index = Index("1","example", ["attribute1"])
        query = Query({}, index)
        self.indexRepository = IndexDynamoPlusRepository(self.collection, index)
        result = self.indexRepository.find(query)
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data), 9)

    def test_indexing(self):
        index = Index("1", "example", ["attribute1"])
        self.indexRepository = IndexDynamoPlusRepository(self.collection, index)
        result = self.indexRepository.create({"id": "1", "attribute1": "100"})
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk())
        self.assertIsNotNone(result.sk())
        self.assertIsNotNone(result.data())
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk(), "example#1")
        self.assertEqual(result.sk(), "example#attribute1")
        self.assertEqual(result.data(), "100")


    # NOT SUPPORTED BY moto    
    # def test_query_with_ordering(self):
    #     for i in range(1,10):
    #         document = {"id": str(i), "attribute1": str(i%2), "attribute2":"value_"+str(i)}
    #         self.table.put_item(Item={"pk":"example#"+str(i),"sk":"example","data":str(i),"document":json.dumps(document)})
    #         self.table.put_item(Item={"pk":"example#"+str(i),"sk":"example#attribute2__ORDER_BY__attribute1","data":str(i)+"#"+str(i%2),"document":json.dumps(document)})
    #     index = Index("example",["attribute2"],"attribute1")
    #     query = Query({"attribute2":"1"},index)
    #     self.indexRepository = IndexDynamoPlusRepository(self.collection,index)
    #     result = self.indexRepository.find(query)
    #     self.assertIsNotNone(result)
    #     self.assertEqual(len(result.data),1)
    #     for r in result.data:
    #         self.assertEqual(r.document["attribute1"],'1')
