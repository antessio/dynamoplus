import unittest
import uuid
from datetime import datetime

from moto import mock_dynamodb2

from dynamoplus.v2.repository.repositories import DynamoDBRepositoryRepository, \
    IndexingOperation, IndexModel
from dynamoplus.v2.repository.system_repositories import ClientAuthorizationEntity, IndexByCollectionNameEntity
from test.common_test_utils import set_up_for_integration_test, cleanup_table, get_dynamodb_table

table_name = "system"


@mock_dynamodb2
class TestClientAuthorizationRepository(unittest.TestCase):

    @mock_dynamodb2
    def setUp(self):
        set_up_for_integration_test(table_name)
        self.repository = DynamoDBRepositoryRepository(table_name)

    def tearDown(self):
        cleanup_table(table_name)

    def test_create_client_authorization(self):
        expected_id = "client_1"
        expected_object = {
            "type": "http_signature",
            "public_key": "public_key"
        }
        result = self.repository.create(ClientAuthorizationEntity(expected_id, expected_object))
        self.assertIsInstance(result, ClientAuthorizationEntity)
        self.assertIsNotNone(result)
        self.assertEqual(result.id(), expected_id)
        self.assertDictEqual(result.object(), expected_object)
        self.assertEqual(result.ordering(), None)
        loaded = get_dynamodb_table(table_name).get_item(
            Key={
                'pk': "client_authorization#" + expected_id,
                'sk': "client_authorization"
            })
        self.assertIsNotNone(loaded)
        d = loaded["Item"]["document"]
        self.assertDictEqual(d, expected_object)

    def test_get_client_authorization(self):
        client_id = "1234"
        document = {"id": client_id, "attribute1": "value1", "ordering": "1", "field1": "A", "field2": "B"}
        get_dynamodb_table(table_name).put_item(
            Item={"pk": ("client_authorization#%s" % client_id), "sk": "client_authorization", "data": "1",
                  "document": document})

        result = self.repository.get(ClientAuthorizationEntity(client_id, None))
        self.assertIsInstance(result, ClientAuthorizationEntity)
        self.assertIsNotNone(result)
        self.assertEqual(result.id(), client_id)
        self.assertDictEqual(result.object(), document)

    def test_indexing_update_index(self):
        collection_name = "book"
        id = uuid.uuid4()
        ordering = str(int(datetime.now().timestamp()) * 1000)
        name = '{0}#author__title__genre'.format(collection_name)
        index_object = {
            "collection": {
                "name": collection_name
            },
            "conditions": [
                "author", "title", "genre"

            ],
            "name": name
        }
        get_dynamodb_table(table_name).put_item(
            Item={"pk": ("index#%s" % str(id)), "sk": "index", "data": str(id) + "#" + ordering,
                  "document": index_object})
        get_dynamodb_table(table_name).put_item(
            Item={"pk": ("index#%s" % str(id)), "sk": "index#collection.name", "data": collection_name + "#" + ordering,
                  "document": index_object})
        update_index_object = {**index_object, "collection": {
            "name": "books"
        }}
        self.repository.indexing(IndexingOperation([],
                                                   [IndexByCollectionNameEntity(id, "books", update_index_object,
                                                                                ordering)],
                                                   []))

        loaded = get_dynamodb_table(table_name).get_item(
            Key={
                'pk': "index#" + str(id),
                'sk': "index#collection.name"
            })
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["Item"]["data"], "books#" + ordering)
        d = loaded["Item"]["document"]
        self.assertDictEqual(d, update_index_object)

    def test_indexing_create_index(self):
        collection_name = "book"
        id = uuid.uuid4()
        ordering = str(int(datetime.now().timestamp()) * 1000)
        name = '{0}#author__title__genre'.format(collection_name)
        index_object = {
            "collection": {
                "name": collection_name
            },
            "conditions": [
                "author", "title", "genre"

            ],
            "name": name
        }
        get_dynamodb_table(table_name).put_item(
            Item={"pk": ("index#%s" % str(id)), "sk": "index", "data": str(id) + "#" + ordering,
                  "document": index_object})

        self.repository.indexing(
            IndexingOperation([],
                              [],
                              [IndexByCollectionNameEntity(id, collection_name, index_object, ordering)]))

        loaded = get_dynamodb_table(table_name).get_item(
            Key={
                'pk': "index#" + str(id),
                'sk': "index#collection.name"
            })
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["Item"]["data"], "book#" + ordering)
        d = loaded["Item"]["document"]
        self.assertDictEqual(d, index_object)

    def test_indexing_delete_index(self):
        collection_name = "book"
        id = uuid.uuid4()
        ordering = str(int(datetime.now().timestamp()) * 1000)
        name = '{0}#author__title__genre'.format(collection_name)
        index_object = {
            "collection": {
                "name": collection_name
            },
            "conditions": [
                "author", "title", "genre"

            ],
            "name": name
        }
        get_dynamodb_table(table_name).put_item(
            Item={"pk": ("index#%s" % str(id)), "sk": "index", "data": str(id) + "#" + ordering,
                  "document": index_object})
        get_dynamodb_table(table_name).put_item(
            Item={"pk": ("index#%s" % str(id)), "sk": "index#collection.name", "data": collection_name + "#" + ordering,
                  "document": index_object})

        self.repository.indexing(
            IndexingOperation([IndexByCollectionNameEntity(id, collection_name, index_object, ordering)],
                              [],
                              []))

        loaded = get_dynamodb_table(table_name).get_item(
            Key={
                'pk': "index#" + str(id),
                'sk': "index#collection.name"
            })
        self.assertIsNotNone(loaded)
        self.assertNotIn("Item",loaded)
