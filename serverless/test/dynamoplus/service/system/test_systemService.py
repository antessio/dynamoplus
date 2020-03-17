import unittest
import decimal

from dynamoplus.models.query.query import Query
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.repository.models import Model, QueryResult
from dynamoplus.service.system.system import SystemService
from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType
from dynamoplus.models.system.index.index import Index
from dynamoplus.models.system.client_authorization.client_authorization import ClientAuthorization, \
    ClientAuthorizationHttpSignature, ClientAuthorizationApiKey, Scope, ScopesType

from mock import call
from unittest.mock import patch


class TestSystemService(unittest.TestCase):

    def setUp(self):
        self.systemService = SystemService()

    @patch.object(DynamoPlusRepository, "update")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_update_authorization_http_signature(self, mock_repository, mock_update):
        expected_client_id = "test"
        client_authorization = ClientAuthorizationHttpSignature(expected_client_id,
                                                                [Scope("example", ScopesType.CREATE)], "my-public-key")
        client_authorization_metadata = Collection("client_authorization", "client_id")
        mock_repository.return_value = None
        document = {
            "type": "http_signature",
            "client_id": expected_client_id,
            "client_scopes": [{"collection_name": "example", "scope_type": "CREATE"}],
            "public_key": "my-public-key"
        }
        mock_update.return_value = Model(client_authorization_metadata, document)
        result = self.systemService.update_authorization(client_authorization)
        self.assertEqual(result.client_id, client_authorization.client_id)
        self.assertTrue(isinstance(result, ClientAuthorizationHttpSignature))
        self.assertEqual(call(document), mock_update.call_args_list[0])

    @patch.object(DynamoPlusRepository, "delete")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_delete_authorization_http_signature(self, mock_repository, mock_delete):
        expected_client_id = "test"
        client_authorization_metadata = Collection("client_authorization", "client_id")
        mock_repository.return_value = None
        document = {
            "type": "http_signature",
            "client_id": expected_client_id,
            "client_scopes": [{"collection_name": "example", "scope_type": "CREATE"}],
            "public_key": "my-public-key"
        }
        mock_delete.return_value = Model(client_authorization_metadata, document)
        self.systemService.delete_authorization(expected_client_id)
        self.assertEqual(call(expected_client_id), mock_delete.call_args_list[0])

    @patch.object(DynamoPlusRepository, "create")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_create_authorization_http_signature(self, mock_repository, mock_create):
        expected_client_id = "test"
        client_authorization = ClientAuthorizationHttpSignature(expected_client_id,
                                                                [Scope("example", ScopesType.CREATE)], "my-public-key")
        client_authorization_metadata = Collection("client_authorization", "client_id")
        mock_repository.return_value = None
        document = {
            "type": "http_signature",
            "client_id": expected_client_id,
            "client_scopes": [{"collection_name": "example", "scope_type": "CREATE"}],
            "public_key": "my-public-key"
        }
        mock_create.return_value = Model(client_authorization_metadata, document)
        result = self.systemService.create_client_authorization(client_authorization)
        self.assertEqual(result.client_id, client_authorization.client_id)
        self.assertTrue(isinstance(result, ClientAuthorizationHttpSignature))
        self.assertEqual(call(document), mock_create.call_args_list[0])

    @patch.object(DynamoPlusRepository, "create")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_create_authorization_api_key(self, mock_repository, mock_create):
        expected_client_id = "test"
        client_authorization = ClientAuthorizationApiKey(expected_client_id,
                                                         [Scope("example", ScopesType.CREATE)], "my-api-key", [])
        client_authorization_metadata = Collection("client_authorization", "client_id")
        mock_repository.return_value = None
        document = {
            "type": "api_key",
            "client_id": expected_client_id,
            "client_scopes": [{"collection_name": "example", "scope_type": "CREATE"}],
            "api_key": "my-api-key"
        }
        mock_create.return_value = Model(client_authorization_metadata, document)
        result = self.systemService.create_client_authorization(client_authorization)
        self.assertEqual(result.client_id, client_authorization.client_id)
        self.assertTrue(isinstance(result, ClientAuthorizationApiKey))
        self.assertEqual(call(document), mock_create.call_args_list[0])

    @patch.object(DynamoPlusRepository, "get")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_get_client_authorization_http_signature(self, mock_repository, mock_get):
        expected_client_id = 'my-client-id'
        mock_repository.return_value = None
        client_authorization_metadata = Collection("client_authorization", "client_id")
        document = {"client_id": expected_client_id, "type": "http_signature", "public_key": "my-public-key",
                    "client_scopes": [{"collection_name": "example", "scope_type": "GET"}]}
        expected_model = Model(client_authorization_metadata, document)
        mock_get.return_value = expected_model
        result = self.systemService.get_client_authorization(expected_client_id)
        self.assertTrue(mock_get.called_with(expected_client_id))
        self.assertEqual(expected_client_id, result.client_id)
        self.assertIsInstance(result, ClientAuthorizationHttpSignature)
        self.assertEqual("my-public-key", result.client_public_key)
        self.assertEqual(1, len(result.client_scopes))
        self.assertEqual("example", result.client_scopes[0].collection_name)
        self.assertEqual("GET", result.client_scopes[0].scope_type.name)

    @patch.object(DynamoPlusRepository, "get")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_get_client_authorization_api_key(self, mock_repository, mock_get):
        expected_client_id = 'my-client-id'
        mock_repository.return_value = None
        client_authorization_metadata = Collection("client_authorization", "client_id")
        document = {"client_id": expected_client_id, "type": "api_key",
                    "api_key": "my_api_key",
                    "whitelist_hosts": ["*"],
                    "client_scopes": [{"collection_name": "example", "scope_type": "GET"}]}
        expected_model = Model(client_authorization_metadata, document)
        mock_get.return_value = expected_model
        result = self.systemService.get_client_authorization(expected_client_id)
        self.assertTrue(mock_get.called_with(expected_client_id))
        self.assertEqual(expected_client_id, result.client_id)
        self.assertIsInstance(result, ClientAuthorizationApiKey)

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
        target_index = {"uid": "1", "name": expected_id, "collection": {"name": "example"},
                        "conditions": expected_conditions,
                        "ordering_key": "field2.field21"}
        index_metadata = Collection("index", "name")
        expected_model = Model(index_metadata, target_index)
        mock_repository.return_value = None
        mock_create.return_value = expected_model
        index = Index("1", "example", expected_conditions, "field2.field21")
        created_index = self.systemService.create_index(index)
        index_name = created_index.index_name
        self.assertEqual(index_name, expected_id)
        self.assertEqual(call(target_index), mock_create.call_args_list[0])

    @patch.object(DynamoPlusRepository, "create")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_createIndexWithNoOrdering(self, mock_repository, mock_create):
        expected_id = 'field1__field2.field21'
        expected_conditions = ["field1", "field2.field21"]
        target_index = {"uid": "1", "name": expected_id, "collection": {"name": "example"},
                        "conditions": expected_conditions, "ordering_key": None}
        index_metadata = Collection("index", "name")
        expected_model = Model(index_metadata, target_index)
        mock_repository.return_value = None
        mock_create.return_value = expected_model
        index = Index("1", "example", expected_conditions)
        created_index = self.systemService.create_index(index)
        index_name = created_index.index_name
        self.assertEqual(index_name, expected_id)
        self.assertEqual(call(target_index), mock_create.call_args_list[0])

    @patch.object(IndexDynamoPlusRepository, "find")
    @patch.object(IndexDynamoPlusRepository, "__init__")
    def test_queryCollectionByName(self, mock_index_dynamoplus_repository, mock_find):
        index = Index("1", "collection", ["name"])
        expected_query = Query({"name": "example"}, index)
        collection_metadata = Collection("example", "name")
        mock_index_dynamoplus_repository.return_value = None
        mock_find.return_value = QueryResult([Model(Collection("example", "id"), {"name": "example", "id_key": "id"})])
        collections = self.systemService.find_collections_by_example(collection_metadata)
        self.assertTrue(len(collections) == 1)
        self.assertEqual(collections[0].name, "example")
        self.assertEqual(call(expected_query), mock_find.call_args_list[0])

    @patch.object(IndexDynamoPlusRepository, "find")
    @patch.object(IndexDynamoPlusRepository, "__init__")
    def test_queryIndex_by_CollectionByName(self, mock_index_dynamoplus_repository, mock_find):
        index = Index("1", "index", ["collection.name"])
        expected_query = Query({"collection": {"name": "example"}}, index)
        mock_index_dynamoplus_repository.return_value = None
        mock_find.return_value = QueryResult(
            [Model(Collection("index", "name"),
                   {"uid": "1", "name": "collection.name", "collection": {"name": "example"},
                    "conditions": ["collection.name"]})])
        indexes, last_key = self.systemService.find_indexes_from_collection_name("example")
        self.assertEqual(1, len(indexes))
        self.assertEqual(indexes[0].uid, "1")
        self.assertEqual(indexes[0].index_name, "collection.name")
        self.assertEqual(call(expected_query), mock_find.call_args_list[0])

    @patch.object(IndexDynamoPlusRepository, "find")
    @patch.object(IndexDynamoPlusRepository, "__init__")
    def test_queryIndex_by_CollectionByName_generator(self, mock_index_dynamoplus_repository, mock_find):
        index = Index("1", "index", ["collection.name"])
        expected_query = Query({"collection": {"name": "example"}}, index)
        mock_index_dynamoplus_repository.return_value = None
        mock_find.side_effect = [
            self.fake_query_result("1", "2"),
            self.fake_query_result("2", "3"),
            self.fake_query_result("3", "4"),
            self.fake_query_result("4", "5"),
            self.fake_query_result("5"),
        ]
        indexes = self.systemService.get_indexes_from_collection_name_generator("example", 2)
        uids = list(map(lambda i: i.uid, indexes))
        self.assertEqual(5, len(uids))
        self.assertEqual(["1", "2", "3", "4", "5"], uids)
        self.assertEqual(call(expected_query), mock_find.call_args_list[0])

    @patch.object(IndexDynamoPlusRepository, "find")
    @patch.object(IndexDynamoPlusRepository, "__init__")
    def test_queryIndex_by_CollectionByName_generator(self, mock_index_dynamoplus_repository, mock_find):
        index = Index("1", "index", ["collection.name"])
        expected_query = Query({"collection": {"name": "example"}}, index)
        mock_index_dynamoplus_repository.return_value = None
        mock_find.side_effect = [
            self.fake_query_result("1", "2"),
            self.fake_query_result("2", "3"),
            self.fake_query_result("3", "4"),
            self.fake_query_result("4", "5"),
            self.fake_query_result("5"),
        ]
        indexes = self.systemService.get_indexes_from_collection_name_generator("example", 2)
        uids = list(map(lambda i: i.uid, indexes))
        self.assertEqual(5, len(uids))
        self.assertEqual(["1", "2", "3", "4", "5"], uids)
        self.assertEqual(call(expected_query), mock_find.call_args_list[0])

    def fake_query_result(self, uid, next=None):
        return QueryResult(
            [Model(Collection("index", "name"),
                   {"uid": uid, "name": "collection.name", "collection": {"name": "example" + uid},
                    "conditions": ["collection.name"]})], next)
