import unittest
import uuid
from datetime import datetime
from typing import Callable, Type

import parameterized as parameterized
from moto import mock_dynamodb2

from dynamoplus.v2.repository.repositories import DynamoDBRepositoryRepository, \
    IndexingOperation, IndexModel, Model, Query
from dynamoplus.v2.repository.system_repositories import ClientAuthorizationEntity, IndexByCollectionNameEntity, \
    IndexEntity, CollectionEntity, QueryIndexByCollectionName, IndexByCollectionNameAndFieldsEntity, \
    QueryIndexByCollectionNameAndFields, AggregationConfigurationByCollectionNameEntity, \
    QueryAggregationConfigurationByCollectionName, AggregationConfigurationEntity
from test.common_test_utils import set_up_for_integration_test, cleanup_table, get_dynamodb_table

from boto3.dynamodb.conditions import Key, ComparisonCondition

table_name = "system"


@mock_dynamodb2
class TestSystemRepository(unittest.TestCase):

    @mock_dynamodb2
    def setUp(self):
        set_up_for_integration_test(table_name)

    def tearDown(self):
        cleanup_table(table_name)

    @parameterized.parameterized.expand([
        ("client_authorization", ClientAuthorizationEntity("client_1", {
            "type": "http_signature",
            "public_key": "public_key"
        })),
        ("index", IndexEntity(uuid.uuid4(), {
            "name": "book_by_author",
            "collection": {"name": "book"},
            "conditions": ["author"],
            "ordering_key": "publishing_date"
        })),
        ("collection", CollectionEntity("book", {
            "name": "book",
            "ordering": "publishing_date"
        }))
    ])
    def test_create(self, collection_name, expected_entity):
        repository = DynamoDBRepositoryRepository(table_name, expected_entity.__class__)
        result = repository.create(expected_entity)
        self.assertIsInstance(result, expected_entity.__class__)
        self.assertIsNotNone(result)
        self.assertEqual(result.id(), expected_entity.id())
        self.assertDictEqual(result.object(), expected_entity.object())
        self.assertEqual(result.ordering(), expected_entity.ordering())
        loaded = get_dynamodb_table(table_name).get_item(
            Key={
                'pk': collection_name + "#" + str(expected_entity.id()),
                'sk': collection_name
            })
        self.assertIsNotNone(loaded)
        self.assertIn("Item", loaded)
        d = loaded["Item"]["document"]
        self.assertDictEqual(d, expected_entity.object())

    @parameterized.parameterized.expand([
        ("client_authorization",
         ClientAuthorizationEntity("client_1", None),
         {
             "type": "http_signature",
             "public_key": "public_key"
         }),
        ("index", IndexEntity(uuid.uuid4(), None),
         {
             "name": "book_by_author",
             "collection": {"name": "book"},
             "conditions": ["author"],
             "ordering_key": "publishing_date"
         }),
        ("collection", CollectionEntity("book", None),
         {
             "name": "book",
             "ordering": "publishing_date"
         })
    ])
    def test_get(self, collection_name: str, expected_entity_key: Model, expected_document: dict):
        repository = DynamoDBRepositoryRepository(table_name, expected_entity_key.__class__)
        get_dynamodb_table(table_name).put_item(
            Item={"pk": (collection_name + "#%s" % expected_entity_key.id()), "sk": collection_name,
                  "data": expected_entity_key.id(),
                  "document": expected_document})

        result = repository.get(expected_entity_key)
        self.assertIsInstance(result, expected_entity_key.__class__)
        self.assertIsNotNone(result)
        self.assertEqual(result.id(), expected_entity_key.id())
        self.assertDictEqual(result.object(), expected_document)

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
        repository = DynamoDBRepositoryRepository(table_name, IndexEntity)
        repository.indexing(IndexingOperation([],
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
        repository = DynamoDBRepositoryRepository(table_name, IndexEntity)
        repository.indexing(
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
        expected_index = IndexByCollectionNameEntity(id, collection_name, index_object, ordering)
        get_dynamodb_table(table_name).put_item(
            Item=expected_index.to_dynamo_db_model().to_dynamo_db_item())
        repository = DynamoDBRepositoryRepository(table_name, IndexEntity)
        repository.indexing(
            IndexingOperation([expected_index],
                              [],
                              []))

        loaded = get_dynamodb_table(table_name).get_item(
            Key={
                'pk': "index#" + str(id),
                'sk': "index#collection.name"
            })
        self.assertIsNotNone(loaded)
        self.assertNotIn("Item", loaded)

    @parameterized.parameterized.expand([
        ("index_by_collection_name",
         {
             "collection": {
                 "name": "book"
             },
             "conditions": [
                 "author", "title", "genre"

             ],
             "name": "book#author__title__genre"
         },
         str(int(datetime.now().timestamp()) * 1000),
         lambda index_id, index_object, ordering: IndexByCollectionNameEntity(index_id, "book", index_object, ordering),
         QueryIndexByCollectionName("book"),
         IndexEntity),
        ("index_by_collection_name_and_fields",
         {
             "collection": {
                 "name": "book"
             },
             "conditions": [
                 "author", "title", "genre"

             ],
             "name": "book#author__title__genre"
         },
         str(int(datetime.now().timestamp()) * 1000),
         lambda index_id, index_object, ordering: IndexByCollectionNameAndFieldsEntity(index_id, 'book', ['author', 'title', 'genre'],index_object,ordering),
         QueryIndexByCollectionNameAndFields("book", ['author', 'title', 'genre']),
         IndexEntity),
        ("aggregation_configuration_by_collection_name_and_fields",
         {
             "collection": {
                 "name": "book"
             },
             "name": "book_count",
             "type": "COUNT",
             "count": 100
         },
         str(int(datetime.now().timestamp()) * 1000),
         lambda index_id, index_object, ordering: AggregationConfigurationByCollectionNameEntity(index_id, 'book', index_object, ordering),
         QueryAggregationConfigurationByCollectionName("book"),
         AggregationConfigurationEntity)
    ])
    def test_query_by_field_eq(self, name: str, index_object: dict, ordering: str,
                               index_model_builder: Callable[[uuid, dict, str], IndexModel], query: Query, target_entity:Type):
        index_id = uuid.uuid4()
        get_dynamodb_table(table_name).put_item(
            Item={"pk": ("index#%s" % str(index_id)), "sk": "index", "data": str(index_id) + "#" + ordering,
                  "document": index_object})
        get_dynamodb_table(table_name).put_item(
            Item=index_model_builder(index_id, index_object, ordering).to_dynamo_db_model().to_dynamo_db_item())

        repository = DynamoDBRepositoryRepository(table_name, target_entity)
        result, last_key = repository.query(query, 10)
        self.assertIsNotNone(result)
        self.assertIsNone(last_key)
        self.assertEqual(len(result), 1)
        self.assertDictEqual(result[0].object(), index_object)

