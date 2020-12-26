import unittest
from decimal import Decimal

from dynamoplus.v2.repository.repositories import Repository, Model, QueryRepository, AtomicIncrement, Counter
from dynamoplus.models.system.collection.collection import Collection
from moto import mock_dynamodb2
import json
import boto3
import os

table_name = "example-domain"


@mock_dynamodb2
class TestDynamoPlusRepository(unittest.TestCase):

    @mock_dynamodb2
    def setUp(self):
        os.environ["TEST_FLAG"] = "true"
        os.environ["DYNAMODB_DOMAIN_TABLE"] = table_name
        self.dynamodb = boto3.resource("dynamodb")
        self.dynamodb.create_table(TableName=table_name,
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
        if "DYNAMODB_DOMAIN_TABLE" in os.environ:
            del os.environ["DYNAMODB_DOMAIN_TABLE"]

    def test_create(self):
        repository = Repository(table_name)
        model = Model("example#randomUid", "example", "1",
                      {"id": "randomUid", "ordering": "1", "field1": "A", "field2": "B"})
        result = repository.create(model)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk)
        self.assertIsNotNone(result.sk)
        self.assertIsNotNone(result.data)
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk, "example#randomUid")
        self.assertEqual(result.sk, "example")
        self.assertEqual(result.data, "1")
        self.assertDictEqual(result.document, model.document)


    def test_increment_counter_multiple(self):
        repository = Repository(table_name)
        document = {"id": "1234", "attribute1": 1,"attribute2":2, "ordering": "1", "field1": "A", "field2": "B"}
        pk = "example#1234"
        sk = "example"
        self.table.put_item(
            Item={"pk": pk, "sk": sk, "data": "1234", "document": document })

        result = repository.increment_counter(AtomicIncrement(pk,sk,[Counter("attribute1",Decimal(3)),Counter("attribute2",Decimal(6))]))
        self.assertIsNotNone(result)
        self.assertTrue(result)

        loaded = self.table.get_item(
            Key={
                'pk': pk,
                'sk': sk
            })
        self.assertIsNotNone(loaded)
        d = loaded["Item"]["document"]
        self.assertEqual(d["attribute1"], 4)
        self.assertEqual(d["attribute2"], 8)


    def test_increment_counter(self):
        repository = Repository(table_name)
        document = {"id": "1234", "attribute1": 1, "ordering": "1", "field1": "A", "field2": "B"}
        pk = "example#1234"
        sk = "example"
        self.table.put_item(
            Item={"pk": pk, "sk": sk, "data": "1234", "document": document })

        result = repository.increment_counter(AtomicIncrement(pk,sk,[Counter("attribute1",Decimal(3))]))
        self.assertIsNotNone(result)
        self.assertTrue(result)

        loaded = self.table.get_item(
            Key={
                'pk': pk,
                'sk': sk
            })
        self.assertIsNotNone(loaded)
        d = loaded["Item"]["document"]
        self.assertEqual(d["attribute1"], Decimal(4))


    def test_increment_counter_decrease(self):
        repository = Repository(table_name)
        document = {"id": "1234", "attribute1": 4, "ordering": "1", "field1": "A", "field2": "B"}
        pk = "example#1234"
        sk = "example"
        self.table.put_item(
            Item={"pk": pk, "sk": sk, "data": "1234", "document": document })

        result = repository.increment_counter(AtomicIncrement(pk,sk,[Counter("attribute1",Decimal(3),False)]))
        self.assertIsNotNone(result)
        self.assertTrue(result)

        loaded = self.table.get_item(
            Key={
                'pk': pk,
                'sk': sk
            })
        self.assertIsNotNone(loaded)
        d = loaded["Item"]["document"]
        self.assertEqual(d["attribute1"], Decimal(1))


    def test_update(self):
        repository = Repository(table_name)
        document = {"id": "1234", "attribute1": "value1", "ordering": "1", "field1": "A", "field2": "B"}
        pk = "example#1234"
        sk = "example"
        model = Model(pk, sk, "1",
                      document)
        self.table.put_item(
            Item={"pk": pk, "sk": sk, "data": "1234", "document": document})
        document["attribute1"] = "value2"
        result = repository.update(model)

        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk)
        self.assertIsNotNone(result.sk)
        self.assertIsNotNone(result.data)
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk, pk)
        self.assertEqual(result.sk, sk)
        self.assertEqual(result.data, "1")
        self.assertDictEqual(result.document, document)
        self.assertEqual(result.document["attribute1"], "value2")
        loaded = self.table.get_item(
            Key={
                'pk': pk,
                'sk': sk
            })
        self.assertIsNotNone(loaded)
        d = loaded["Item"]["document"]

    def test_delete(self):
        repository = Repository(table_name)
        document = {"id": "1234", "attribute1": "value1", "ordering": "1", "field1": "A", "field2": "B"}
        self.table.put_item(Item={"pk": "example#1234", "sk": "example", "data": "1", **document})
        repository.delete("example#1234", "example")
        result = repository.get("example#1234", "example")
        self.assertIsNone(result)

    def test_get(self):
        document = {"id": "1234", "attribute1": "value1", "ordering": "1", "field1": "A", "field2": "B"}
        self.table.put_item(
            Item={"pk": "example#1234", "sk": "example", "data": "1", "document": document})
        repository = Repository(table_name)
        result = repository.get("example#1234", "example")
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.pk)
        self.assertIsNotNone(result.sk)
        self.assertIsNotNone(result.data)
        self.assertIsNotNone(result.document)
        self.assertEqual(result.pk, "example#1234")
        self.assertEqual(result.sk, "example")
        self.assertEqual(result.data, "1")
        self.assertDictEqual(result.document, document)

    def test_query_begins_with(self):
        repository = QueryRepository(table_name)
        for i in range(1, 10):
            document = {"id": str(i), "attribute1": str(i % 2), "attribute2": "value_" + str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": document})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#attribute1", "data": str(i % 2),
                                      "document": document})

        result = repository.query_begins_with("example#attribute1", "1")
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data), 5)
        for r in result.data:
            self.assertEqual(r.document["attribute1"], '1')

    def test_query_begins_with_2(self):
        repository = QueryRepository(table_name)
        for i in range(1, 10):
            document = {"id": str(i), "attribute1": str(i % 2), "attribute2": "value_" + str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": document})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#attribute1#attribute2", "data": str(i % 2)+"#"+str(i),
                                      "document": document})

        result = repository.query_begins_with("example#attribute1#attribute2", "1")
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data), 5)
        for r in result.data:
            self.assertEqual(r.document["attribute1"], '1')

    def test_query_all(self):
        repository = QueryRepository(table_name)
        for i in range(1, 10):
            document = {"id": str(i), "attribute1": str(i % 2), "attribute2": "value_" + str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": document})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#attribute1", "data": str(i % 2),
                                      "document": document})

        result = repository.query_all("example")
        self.assertIsNotNone(result)
        self.assertEqual(len(result.data), 9)


    def test_query_range(self):
        repository = QueryRepository(table_name)
        for i in range(1, 10):
            document = {"id": str(i), "attribute1": str(i % 2), "attribute2": "value_" + str(i), "attribute3": str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": document})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#attribute1#attribute3",
                                      "data": str(i % 2) + "#" + str(i),
                                      "document": document})

        result = repository.query_range("example#attribute1#attribute3", "1#3","1#7")
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

    def test_query_limit(self):
        for i in range(1, 50):
            document = {"id": f"{i:08}", "attribute1": str(i % 2), "attribute2": "value_" + f"{i:08}",
                        "attribute3": f"{i:08}"}
            self.table.put_item(
                Item={"pk": "example#" + document["id"], "sk": "example", "data": document["id"],
                      "document": document})
            self.table.put_item(Item={"pk": "example#" + document["id"],
                                      "sk": "example#attribute1#attribute3",
                                      "data": str(i % 2) + "#" + f"{i:08}",
                                      "document": document})
        repository = QueryRepository(table_name)
        result = repository.query_range("example#attribute1#attribute3","1#00000020","1#00000030",3)
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

    def test_query_limit_starting_after(self):
        for i in range(1, 50):
            document = {"id": f"{i:08}", "attribute1": str(i % 2), "attribute2": "value_" + f"{i:08}",
                        "attribute3": f"{i:08}"}
            self.table.put_item(
                Item={"pk": "example#" + document["id"], "sk": "example", "data": document["id"],
                      "document": document})
            self.table.put_item(Item={"pk": "example#" + document["id"],
                                      "sk": "example#attribute1#attribute3",
                                      "data": str(i % 2) + "#" + f"{i:08}",
                                      "document": document})
        repository = QueryRepository(table_name)
        result = repository.query_range("example#attribute1#attribute3","1#00000020","1#00000030",3)
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
