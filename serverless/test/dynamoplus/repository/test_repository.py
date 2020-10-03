import unittest
from typing import *

from dynamoplus.models.query.conditions import Eq, And, Range
from dynamoplus.models.query.query import Index
from dynamoplus.repository.models import Query, IndexModel, Model
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.models.system.collection.collection import Collection
from moto import mock_dynamodb2
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

        self.table = self.dynamodb.Table('example-domain')

    def tearDown(self):
        self.table.delete()
        del os.environ["DYNAMODB_DOMAIN_TABLE"]

    def test_create(self):
        repository = DynamoPlusRepository(self.collection)
        document = {"id": "randomUid", "ordering": "1", "field1": "A", "field2": "B"}
        result = repository.create(document)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk)
        self.assertIsNotNone(result.sk)
        self.assertIsNotNone(result.data)
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk(), "example#randomUid")
        self.assertEqual(result.sk(), "example")
        self.assertEqual(result.data(), "1")
        self.assertDictEqual(result.document, document)

    def test_create_index(self):
        document = {"id": "randomUid", "ordering": "1", "field1": "A", "field2": "B"}
        repository = IndexDynamoPlusRepository(self.collection, Index("1", "example", ["field1", "field2"], "ordering"))
        result = repository.create(document)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk)
        self.assertIsNotNone(result.sk)
        self.assertIsNotNone(result.data)
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk(), "example#randomUid")
        self.assertEqual(result.sk(), "example#field1#field2")
        self.assertEqual(result.data(), "A#B#1")
        self.assertDictEqual(result.document, document)

    def test_update(self):
        repository = DynamoPlusRepository(self.collection)
        document = {"id": "1234", "attribute1": "value1", "ordering": "1", "field1": "A", "field2": "B"}
        self.table.put_item(
            Item={"pk": "example#1234", "sk": "example", "data": "1234", "document": json.dumps(document)})
        document["attribute1"] = "value2"
        result = repository.update(document)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk())
        self.assertIsNotNone(result.sk())
        self.assertIsNotNone(result.data())
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk(), "example#1234")
        self.assertEqual(result.sk(), "example")
        self.assertEqual(result.data(), "1")
        self.assertDictEqual(result.document, document)
        self.assertEqual(result.document["attribute1"], "value2")

    def test_update_index(self):
        document = {"id": "1234", "attribute1": "value1", "ordering": "1", "field1": "A", "field2": "B"}
        ## index repository
        self.table.put_item(
            Item={"pk": "example#1234", "sk": "example#field1#field2", "data": "A#B#1",
                  "document": json.dumps(document)})
        repository = IndexDynamoPlusRepository(self.collection, Index("1", "example", ["field1", "field2"], "ordering"))
        document["field1"] = "X"
        document["attribute1"] = "value2"
        result = repository.update(document)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk())
        self.assertIsNotNone(result.sk())
        self.assertIsNotNone(result.data())
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk(), "example#1234")
        self.assertEqual(result.sk(), "example#field1#field2")
        self.assertEqual(result.data(), "X#B#1")
        self.assertDictEqual(result.document, document)
        self.assertEqual(result.document["attribute1"], "value2")
        self.assertEqual(result.document["field1"], "X")

    def test_delete(self):
        repository = DynamoPlusRepository(self.collection)
        document = {"id": "1234", "attribute1": "value1", "ordering": "1", "field1": "A", "field2": "B"}
        self.table.put_item(Item={"pk": "example#1234", "sk": "example", "data": "1", **document})
        repository.delete("1234")
        result = repository.get("1234")
        self.assertIsNone(result)

    def test_delete_index(self):
        ## index repository
        document = {"id": "1234", "attribute1": "value1", "ordering": "1", "field1": "A", "field2": "B"}
        self.table.put_item(
            Item={"pk": "example#1234", "sk": "example#field1#field2", "data": "A#B#1",
                  "document": json.dumps(document)})
        repository = IndexDynamoPlusRepository(self.collection, Index("1", "example", ["field1", "field2"], "ordering"))
        repository.delete("1234")
        result = repository.get("1234")
        self.assertIsNone(result)

    def test_get(self):
        repository = DynamoPlusRepository(self.collection)
        document = {"id": "1234", "attribute1": "value1", "ordering": "1", "field1": "A", "field2": "B"}
        self.table.put_item(
            Item={"pk": "example#1234", "sk": "example", "data": "1", "document": json.dumps(document)})
        result = repository.get("1234")
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk())
        self.assertIsNotNone(result.sk())
        self.assertIsNotNone(result.data())
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk(), "example#1234")
        self.assertEqual(result.sk(), "example")
        self.assertEqual(result.data(), "1")
        self.assertDictEqual(result.document, document)

    def test_get_index(self):
        document = {"id": "1234", "attribute1": "value1", "ordering": "1", "field1": "A", "field2": "B"}
        ## index
        self.table.put_item(
            Item={"pk": "example#1234", "sk": "example#field1#field2", "data": "A#B#1",
                  "document": json.dumps(document)})
        repository = IndexDynamoPlusRepository(self.collection, Index("1", "example", ["field1", "field2"], "ordering"))
        document = {"id": "1234", "attribute1": "value1", "ordering": "1", "field1": "A", "field2": "B"}
        result = repository.get("1234")
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk())
        self.assertIsNotNone(result.sk())
        self.assertIsNotNone(result.data())
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk(), "example#1234")
        self.assertEqual(result.sk(), "example#field1#field2")
        self.assertEqual(result.data(), "A#B#1")
        self.assertDictEqual(result.document, document)

    def test_query_v2(self):
        repository = DynamoPlusRepository(self.collection)
        for i in range(1, 10):
            document = {"id": str(i), "attribute1": str(i % 2), "attribute2": "value_" + str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#attribute1", "data": str(i % 2),
                                      "document": json.dumps(document)})

        query_model = Query(Eq("attribute1", "1"), self.collection, Index(None, "example",["attribute1"]))
        result = repository.query_v2(query_model)
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data), 5)
        for r in result.data:
            self.assertEqual(r.document["attribute1"], '1')

    def test_query_v2_multiple_conditions(self):
        repository = DynamoPlusRepository(self.collection)
        for i in range(1, 10):
            document = {"id": str(i), "attribute1": str(i % 2), "attribute2": "value_" + str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#attribute1#attribute2",
                                      "data": str(i % 2) + "#value_" + str(i),
                                      "document": json.dumps(document)})

        query_model = Query(And([Eq("attribute1", "1"), Eq("attribute2", "value_3")]), self.collection,
                            Index(None, "example",["attribute1", "attribute2"]))
        result = repository.query_v2(query_model)
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data), 1)
        self.assertEqual("1", result.data[0].document["attribute1"])
        self.assertEqual("value_3", result.data[0].document["attribute2"])

    def test_query_v2_multiple_conditions_range(self):
        repository = DynamoPlusRepository(self.collection)
        for i in range(1, 10):
            document = {"id": str(i), "attribute1": str(i % 2), "attribute2": "value_" + str(i), "attribute3": str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#attribute1#attribute3",
                                      "data": str(i % 2) + "#" + str(i),
                                      "document": json.dumps(document)})

        query_model = Query(And([Eq("attribute1", "1"), Range("attribute3", "3", "7")]), self.collection,
                            Index(None, "example", ["attribute1", "attribute3"]))
        result = repository.query_v2(query_model)
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data), 3)
        self.assertEqual("1", result.data[1].document["attribute1"])
        self.assertEqual("value_3", result.data[0].document["attribute2"])
        self.assertEqual("3", result.data[0].document["attribute3"])
        self.assertEqual("1", result.data[1].document["attribute1"])
        self.assertEqual("value_5", result.data[1].document["attribute2"])
        self.assertEqual("5", result.data[1].document["attribute3"])
        self.assertEqual("1", result.data[2].document["attribute1"])
        self.assertEqual("value_7", result.data[2].document["attribute2"])
        self.assertEqual("7", result.data[2].document["attribute3"])

    def test_query_v2_multiple_conditions_range_limit(self):
        repository = DynamoPlusRepository(self.collection)
        for i in range(1, 50):
            document = {"id": f"{i:08}", "attribute1": str(i % 2), "attribute2": "value_" + f"{i:08}",
                        "attribute3": f"{i:08}"}
            self.table.put_item(
                Item={"pk": "example#" + document["id"], "sk": "example", "data": document["id"],
                      "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + document["id"],
                                      "sk": "example#attribute1#attribute3",
                                      "data": str(i % 2) + "#" + f"{i:08}",
                                      "document": json.dumps(document)})

        query_model = Query(
            And([Eq("attribute1", "1"), Range("attribute3", "00000020", "00000030")]),
            self.collection,
            Index(None, "example", ["attribute1", "attribute3"]),
            3)
        result = repository.query_v2(query_model)
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data), 3)
        self.assertEqual("1", result.data[1].document["attribute1"])
        self.assertEqual("value_00000021", result.data[0].document["attribute2"])
        self.assertEqual("00000021", result.data[0].document["attribute3"])
        self.assertEqual("1", result.data[1].document["attribute1"])
        self.assertEqual("value_00000023", result.data[1].document["attribute2"])
        self.assertEqual("00000023", result.data[1].document["attribute3"])
        self.assertEqual("1", result.data[2].document["attribute1"])
        self.assertEqual("value_00000025", result.data[2].document["attribute2"])
        self.assertEqual("00000025", result.data[2].document["attribute3"])

    def test_query_v2_multiple_conditions_range_limit_start_from(self):
        repository = DynamoPlusRepository(self.collection)
        for i in range(1, 50):
            document = {"id": f"{i:08}", "attribute1": str(i % 2), "attribute2": "value_" + f"{i:08}",
                        "attribute3": f"{i:08}"}
            if i == 27:
                starting_after_document = document
            self.table.put_item(
                Item={"pk": "example#" + document["id"], "sk": "example", "data": document["id"],
                      "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + document["id"],
                                      "sk": "example#attribute1#attribute3",
                                      "data": str(i % 2) + "#" + f"{i:08}",
                                      "document": json.dumps(document)})

        # starting_after_document = {"id": "00000029", "attribute1": "1", "attribute2": "value_00000029", "attribute3": "00000029"}
        index = Index("1", self.collection.name, ["attribute1", "attribute3"])
        starting_after = IndexModel(self.collection, starting_after_document, index)
        query_model = Query(
            And([Eq("attribute1", "1"), Range("attribute3", "00000020", "00000040")]),
            self.collection,
            Index(None, "example", ["attribute1", "attribute3"]),
            3,
            starting_after)
        result = repository.query_v2(query_model)
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data), 3)
        self.assertEqual("1", result.data[1].document["attribute1"])
        self.assertEqual("value_00000029", result.data[0].document["attribute2"])
        self.assertEqual("00000029", result.data[0].document["attribute3"])
        self.assertEqual("1", result.data[1].document["attribute1"])
        self.assertEqual("value_00000031", result.data[1].document["attribute2"])
        self.assertEqual("00000031", result.data[1].document["attribute3"])
        self.assertEqual("1", result.data[2].document["attribute1"])
        self.assertEqual("value_00000033", result.data[2].document["attribute2"])
        self.assertEqual("00000033", result.data[2].document["attribute3"])

    def test_query_v2_subset_of_conditions_matching_index(self):
        repository = DynamoPlusRepository(self.collection)
        for i in range(1, 10):
            document = {"id": str(i), "attribute1": str(i % 2), "attribute2": "value_" + str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#attribute1#attribute2",
                                      "data": str(i % 2) + "#value_" + str(i),
                                      "document": json.dumps(document)})

        query_model = Query(And([Eq("attribute1", "1")]), self.collection,
                            Index(None, "example", ["attribute1", "attribute2"]))
        result = repository.query_v2(query_model)
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data), 5)
        for idx, r in enumerate(result.data):
            self.assertEqual("1", r.document["attribute1"])
            self.assertEqual(1, int(r.document["attribute2"].replace("value_",""))%2)
    #
    # def test_query_all(self):
    #     for i in range(1, 10):
    #         document = {"id": str(i), "attribute1": str(i % 2), "attribute2": "value_" + str(i)}
    #         self.table.put_item(
    #             Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": json.dumps(document)})
    #         self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#attribute1", "data": str(i % 2),
    #                                   "document": json.dumps(document)})
    #     index = Index("1", "example", ["attribute1"])
    #     query = Query({}, index)
    #     self.indexRepository = IndexDynamoPlusRepository(self.collection, index)
    #     result = self.indexRepository.find(query)
    #     self.assertIsNotNone(result)
    #     self.assertEqual(len(result.data), 9)
    #
    # def test_indexing(self):
    #     index = Index("1", "example", ["attribute1"])
    #     self.indexRepository = IndexDynamoPlusRepository(self.collection, index)
    #     result = self.indexRepository.create({"id": "1", "attribute1": "100"})
    #     self.assertIsNotNone(result)
    #     self.assertIsNotNone(result.pk())
    #     self.assertIsNotNone(result.sk())
    #     self.assertIsNotNone(result.data())
    #     self.assertIsNotNone(result.document)
    #     self.assertEqual(result.pk(), "example#1")
    #     self.assertEqual(result.sk(), "example#attribute1")
    #     self.assertEqual(result.data(), "100")

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
