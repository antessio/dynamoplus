import unittest
import decimal

from dynamoplus.models.query.query import Query
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.repository.models import Model, QueryResult
from dynamoplus.service.system.system import SystemService
from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType
from dynamoplus.models.system.index.index import Index
from mock import call
from unittest.mock import patch


class TestSystemService(unittest.TestCase):

    def setUp(self):
        self.systemService = SystemService()

    @patch.object(DynamoPlusRepository, "create")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_createCollection(self, mock_repository, mock_create):
        expected_id = 'example'
        target_collection = {"name": "example", "id_key": "id", "ordering": None}
        document = {"name": expected_id, "id_key": "id"}
        collection_metadata = Collection("collection", "name")
        expected_model = Model(collection_metadata, document)
        mock_repository.return_value = None
        mock_create.return_value = expected_model
        target_metadata = Collection("example", "id")
        created_collection = self.systemService.create_collection(target_metadata)
        collection_id = created_collection.name
        self.assertEqual(collection_id, expected_id)
        self.assertEqual(call(target_collection), mock_create.call_args_list[0])

    # @patch.object(DynamoPlusRepository,"update")
    # @patch.object(DynamoPlusRepository, "__init__")
    # def test_updateCollection(self,mock_repository,mock_update):
    #     expectedId = 'example'
    #     mock_repository.return_value=None
    #     collectionMetadata = Collection("collection","name")
    #     document={"name": expectedId,"idKey":"id"}
    #     expectedModel = Model(collectionMetadata, document)
    #     mock_update.return_value=expectedModel
    #     targetCollection = {"name":expectedId,"fields":[{"field1":"string"}]}
    #     targetMetadata=Collection("example","id",None,[AttributeDefinition("field1",AttributeType.STRING)])
    #     self.systemService.updateCollection(targetMetadata)
    #     self.assertEqual(call(targetCollection),mock_update.call_args_list[0])

    @patch.object(DynamoPlusRepository, "delete")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_deleteCollection(self, mock_repository, mock_delete):
        expected_id = 'example'
        mock_repository.return_value = None
        self.systemService.delete_collection(expected_id)
        self.assertTrue(mock_delete.called_with(expected_id))

    @patch.object(DynamoPlusRepository, "get")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_getCollection(self, mock_repository, mock_get):
        expected_id = 'example'
        mock_repository.return_value = None
        collection_metadata = Collection("collection", "name")
        document = {"name": expected_id, "id_key": expected_id, "fields": [{"field1": "string"}]}
        expected_model = Model(collection_metadata, document)
        mock_get.return_value = expected_model
        result = self.systemService.get_collection_by_name(expected_id)
        # self.assertIn("fields",result)
        self.assertTrue(mock_get.called_with(expected_id))

    @patch.object(DynamoPlusRepository, "create")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_createIndexWithOrdering(self, mock_repository, mock_create):
        expected_id = 'field1__field2.field21__ORDER_BY__field2.field21'
        expected_conditions = ["field1", "field2.field21"]
        target_index = {"uid":"1","name": expected_id, "collection": {"name": "example"}, "conditions": expected_conditions,
                       "ordering_key": "field2.field21"}
        index_metadata = Collection("index", "name")
        expected_model = Model(index_metadata, target_index)
        mock_repository.return_value = None
        mock_create.return_value = expected_model
        index = Index("1","example", expected_conditions, "field2.field21")
        created_index = self.systemService.create_index(index)
        index_name = created_index.index_name
        self.assertEqual(index_name, expected_id)
        self.assertEqual(call(target_index), mock_create.call_args_list[0])

    @patch.object(DynamoPlusRepository, "create")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_createIndexWithNoOrdering(self, mock_repository, mock_create):
        expected_id = 'field1__field2.field21'
        expected_conditions = ["field1", "field2.field21"]
        target_index = {"uid":"1","name": expected_id, "collection": {"name": "example"}, "conditions": expected_conditions, "ordering_key": None}
        index_metadata = Collection("index", "name")
        expected_model = Model(index_metadata, target_index)
        mock_repository.return_value = None
        mock_create.return_value = expected_model
        index = Index("1","example", expected_conditions)
        created_index = self.systemService.create_index(index)
        index_name = created_index.index_name
        self.assertEqual(index_name, expected_id)
        self.assertEqual(call(target_index), mock_create.call_args_list[0])

    @patch.object(IndexDynamoPlusRepository, "find")
    @patch.object(IndexDynamoPlusRepository, "__init__")
    def test_queryCollectionByName(self,mock_index_dynamoplus_repository,mock_find):
        index = Index("1","collection", ["name"])
        expected_query = Query({"name": "example"}, index)
        collection_metadata = Collection("example","name")
        mock_index_dynamoplus_repository.return_value = None
        mock_find.return_value = QueryResult([Model(Collection("example", "id"), {"name": "example", "id_key": "id"})])
        collections = self.systemService.find_collection_by_example(collection_metadata)
        self.assertTrue(len(collections)==1)
        self.assertEqual(collections[0].name,"example")
        self.assertEqual(call(expected_query), mock_find.call_args_list[0])

    @patch.object(IndexDynamoPlusRepository, "find")
    @patch.object(IndexDynamoPlusRepository, "__init__")
    def test_queryIndex_by_CollectionByName(self, mock_index_dynamoplus_repository, mock_find):
        index = Index("1","index", ["collection.name"])
        expected_query = Query({"collection":{"name": "example"}}, index)
        mock_index_dynamoplus_repository.return_value = None
        mock_find.return_value = QueryResult(
            [Model(Collection("index", "name"),
                   {"uid": "1", "name": "collection.name", "collection": {"name": "example"},
                    "conditions": ["collection.name"]})])
        indexes = self.systemService.find_indexes_from_collection_name("example")
        self.assertTrue(len(indexes) == 1)
        self.assertEqual(indexes[0].uid, "1")
        self.assertEqual(indexes[0].index_name, "collection.name")
        self.assertEqual(call(expected_query), mock_find.call_args_list[0])
