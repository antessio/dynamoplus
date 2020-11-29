import os
import unittest
import decimal
import uuid

from dynamoplus.models.query.conditions import Eq
from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType
from dynamoplus.models.system.index.index import Index
from dynamoplus.models.system.client_authorization.client_authorization import ClientAuthorization, \
    ClientAuthorizationHttpSignature, ClientAuthorizationApiKey, Scope, ScopesType

from mock import call
from unittest.mock import patch

from dynamoplus.v2.repository.repositories import Model, Repository, QueryResult
from dynamoplus.v2.service.query_service import QueryService
from dynamoplus.v2.service.system.system_service import AuthorizationService, CollectionService, IndexService

domain_table_name = "domain"
system_table_name = "system"


class TestSystemService(unittest.TestCase):

    def setUp(self):
        os.environ["DYNAMODB_DOMAIN_TABLE"] = domain_table_name
        os.environ["DYNAMODB_SYSTEM_TABLE"] = system_table_name

    @patch.object(Repository, "update")
    @patch.object(Repository, "__init__")
    def test_update_authorization_http_signature(self, mock_repository, mock_update):
        expected_client_id = "test"
        client_authorization = ClientAuthorizationHttpSignature(expected_client_id,
                                                                [Scope("example", ScopesType.CREATE)], "my-public-key")
        collection_name = "client_authorization"
        mock_repository.return_value = None
        document = {
            "type": "http_signature",
            "client_id": expected_client_id,
            "client_scopes": [{"collection_name": "example", "scope_type": "CREATE"}],
            "public_key": "my-public-key"
        }
        expected_model = Model(collection_name + "#" + expected_client_id, collection_name, expected_client_id,
                               document)
        mock_update.return_value = expected_model
        result = AuthorizationService.update_authorization(client_authorization)
        mock_repository.assert_called_once_with(system_table_name)
        self.assertEqual(result.client_id, client_authorization.client_id)
        self.assertTrue(isinstance(result, ClientAuthorizationHttpSignature))
        self.assertEqual(call(expected_model), mock_update.call_args_list[0])

    @patch.object(Repository, "delete")
    @patch.object(Repository, "__init__")
    def test_delete_authorization_http_signature(self, mock_repository, mock_delete):
        expected_client_id = "test"
        collection_name = "client_authorization"
        mock_repository.return_value = None
        mock_delete.return_value = None
        AuthorizationService.delete_authorization(expected_client_id)
        mock_repository.assert_called_once_with(system_table_name)
        self.assertEqual(call(collection_name + "#" + expected_client_id, collection_name),
                         mock_delete.call_args_list[0])

    @patch.object(Repository, "create")
    @patch.object(Repository, "__init__")
    def test_create_authorization_http_signature(self, mock_repository, mock_create):
        expected_client_id = "test"
        client_authorization = ClientAuthorizationHttpSignature(expected_client_id,
                                                                [Scope("example", ScopesType.CREATE)], "my-public-key")
        collection_name = "client_authorization"
        mock_repository.return_value = None
        document = {
            "type": "http_signature",
            "client_id": expected_client_id,
            "client_scopes": [{"collection_name": "example", "scope_type": "CREATE"}],
            "public_key": "my-public-key"
        }
        expected_model = Model(collection_name + "#" + expected_client_id, collection_name, expected_client_id,
                               document)
        mock_create.return_value = expected_model
        result = AuthorizationService.create_client_authorization(client_authorization)
        mock_repository.assert_called_once_with(system_table_name)
        self.assertEqual(result.client_id, client_authorization.client_id)
        self.assertTrue(isinstance(result, ClientAuthorizationHttpSignature))
        self.assertEqual(call(expected_model), mock_create.call_args_list[0])

    @patch.object(Repository, "create")
    @patch.object(Repository, "__init__")
    def test_create_authorization_api_key(self, mock_repository, mock_create):
        expected_client_id = "test"
        client_authorization = ClientAuthorizationApiKey(expected_client_id,
                                                         [Scope("example", ScopesType.CREATE)], "my-api-key", [])
        collection_name = "client_authorization"
        mock_repository.return_value = None
        document = {
            "type": "api_key",
            "client_id": expected_client_id,
            "client_scopes": [{"collection_name": "example", "scope_type": "CREATE"}],
            "api_key": "my-api-key"
        }
        expected_model = Model(collection_name + "#" + expected_client_id, collection_name, expected_client_id,
                               document)
        mock_create.return_value = expected_model
        result = AuthorizationService.create_client_authorization(client_authorization)
        mock_repository.assert_called_once_with(system_table_name)
        self.assertEqual(result.client_id, client_authorization.client_id)
        self.assertTrue(isinstance(result, ClientAuthorizationApiKey))
        self.assertEqual(call(expected_model), mock_create.call_args_list[0])

    @patch.object(Repository, "get")
    @patch.object(Repository, "__init__")
    def test_get_client_authorization_http_signature(self, mock_repository, mock_get):
        expected_client_id = 'my-client-id'
        mock_repository.return_value = None
        collection_name = "client_authorization"
        document = {"client_id": expected_client_id, "type": "http_signature", "public_key": "my-public-key",
                    "client_scopes": [{"collection_name": "example", "scope_type": "GET"}]}

        expected_model = Model(collection_name + "#" + expected_client_id, collection_name, expected_client_id,
                               document)
        mock_get.return_value = expected_model
        result = AuthorizationService.get_client_authorization(expected_client_id)
        mock_repository.assert_called_once_with(system_table_name)
        self.assertTrue(mock_get.called_with(collection_name + "#" + expected_client_id, collection_name))
        self.assertEqual(expected_client_id, result.client_id)
        self.assertIsInstance(result, ClientAuthorizationHttpSignature)
        self.assertEqual("my-public-key", result.client_public_key)
        self.assertEqual(1, len(result.client_scopes))
        self.assertEqual("example", result.client_scopes[0].collection_name)
        self.assertEqual("GET", result.client_scopes[0].scope_type.name)

    @patch.object(Repository, "get")
    @patch.object(Repository, "__init__")
    def test_get_client_authorization_api_key(self, mock_repository, mock_get):
        expected_client_id = 'my-client-id'
        mock_repository.return_value = None
        collection_name = "client_authorization"
        document = {"client_id": expected_client_id, "type": "api_key",
                    "api_key": "my_api_key",
                    "whitelist_hosts": ["*"],
                    "client_scopes": [{"collection_name": "example", "scope_type": "GET"}]}
        expected_model = Model(collection_name + "#" + expected_client_id, collection_name, expected_client_id,
                               document)
        mock_get.return_value = expected_model
        result = AuthorizationService.get_client_authorization(expected_client_id)
        mock_repository.assert_called_once_with(system_table_name)
        self.assertTrue(mock_get.called_with(collection_name + "#" + expected_client_id, collection_name))
        self.assertEqual(expected_client_id, result.client_id)
        self.assertIsInstance(result, ClientAuthorizationApiKey)

    @patch.object(Repository, "create")
    @patch.object(Repository, "__init__")
    def test_createCollection(self, mock_repository, mock_create):
        expected_id = 'example'
        target_collection = {"name": "example", "id_key": "id", "ordering": None, "auto_generate_id": False}
        expected_model = Model("collection#" + expected_id, "collection", expected_id, target_collection)
        mock_repository.return_value = None
        mock_create.return_value = expected_model
        target_metadata = Collection("example", "id")
        created_collection = CollectionService.create_collection(target_metadata)
        mock_repository.assert_called_once_with(system_table_name)
        collection_id = created_collection.name
        self.assertEqual(collection_id, expected_id)
        self.assertEqual(call(expected_model), mock_create.call_args_list[0])

    @patch.object(Repository, "delete")
    @patch.object(Repository, "__init__")
    def test_deleteCollection(self, mock_repository, mock_delete):
        expected_id = 'example'
        mock_repository.return_value = None
        CollectionService.delete_collection(expected_id)
        mock_repository.assert_called_once_with(system_table_name)
        self.assertTrue(mock_delete.called_with("collection#" + expected_id, "collection"))

    @patch.object(Repository, "get")
    @patch.object(Repository, "__init__")
    def test_getCollection(self, mock_repository, mock_get):
        expected_id = 'example'
        mock_repository.return_value = None
        document = {"name": expected_id, "id_key": expected_id, "fields": [{"field1": "string"}]}
        expected_model = Model("collection#" + expected_id, "collection", expected_id, document)
        mock_get.return_value = expected_model
        result = CollectionService.get_collection(expected_id)
        self.assertEqual(result.name, expected_id)
        mock_repository.assert_called_once_with(system_table_name)
        self.assertTrue(mock_get.called_with(expected_id))

    @patch.object(QueryService, "query")
    @patch.object(Repository, "create")
    @patch.object(Repository, "__init__")
    def test_createIndexDuplicated(self, mock_repository, mock_create, mock_query):
        expected_name = 'example__field1__field2.field21__ORDER_BY__field2.field21'
        expected_conditions = ["field1", "field2.field21"]
        target_index = {
            "name": expected_name, "collection": {"name": "example"},
            "conditions": expected_conditions,
            "ordering_key": "field2.field21"}
        mock_repository.return_value = None
        index = Index(None, "example", expected_conditions, "field2.field21")
        expected_index_model = Model("index#" + expected_name, "index", expected_name, target_index)
        mock_query.return_value = QueryResult([expected_index_model], None)
        created_index = IndexService.create_index(index)
        index_name = created_index.index_name
        self.assertEqual(index_name, expected_name)
        self.assertFalse(mock_repository.called)
        self.assertFalse(mock_create.called)
        index_metadata = Collection("index", "name")
        mock_query.assert_called_once_with(index_metadata, Eq("name", expected_name),
                                           Index(None, index_metadata.name, ["name"], None), None, 1)

    @patch.object(QueryService, "query")
    @patch.object(Repository, "create")
    @patch.object(Repository, "__init__")
    def test_createIndexWithOrdering(self, mock_repository, mock_create, mock_query):
        expected_name = 'example__field1__field2.field21__ORDER_BY__field2.field21'
        expected_conditions = ["field1", "field2.field21"]
        expected_id = uuid.uuid4().__str__()
        target_index = {
            "name": expected_name,
            "collection": {"name": "example"},
            "conditions": expected_conditions,
            "ordering_key": "field2.field21"}
        index_metadata = Collection("index", "name")
        # expected_model = Model(index_metadata, target_index)
        expected_index_model = Model("index#" + expected_name, "index", expected_name, target_index)

        mock_repository.return_value = None
        mock_create.return_value = expected_index_model
        mock_query.return_value = QueryResult([], None)
        index = Index(None, "example", expected_conditions, "field2.field21")
        created_index = IndexService.create_index(index)
        index_name = created_index.index_name
        self.assertEqual(index_name, expected_name)
        self.assertEqual(call(expected_index_model), mock_create.call_args_list[0])



    @patch.object(QueryService, "query")
    def test_queryIndex_by_CollectionByName(self, mock_query):
        #expected_query = Query(Eq("collection.name", "example"), Collection("index", "uid"),["collection.name"])
        index_metadata = Collection("index", "name")
        index_by_collection_metadata = Index(None, index_metadata.name, ["collection.name"], None)
        collection_name = "example"
        mock_query.return_value = QueryResult(
            [Model("index#collection.name", "index",collection_name,
                   {"uid": "1", "name": "collection.name", "collection": {"name": collection_name},
                    "conditions": ["collection.name"]})])
        indexes, last_key = IndexService.get_index_by_collection_name(collection_name)
        self.assertEqual(1, len(indexes))
        self.assertEqual(indexes[0].index_name, "example__collection.name")
        self.assertEqual(call(index_metadata,Eq("collection.name", collection_name),index_by_collection_metadata,None,20), mock_query.call_args_list[0])

    @patch.object(QueryService, "query")
    def test_queryIndex_by_CollectionByName_generator(self, mock_query):
        mock_query.side_effect = [
            self.fake_query_result("example__field1", ["field1"], "example","example__field2"),
            self.fake_query_result("example__field2", ["field2"],"example","example__field3"),
            self.fake_query_result("example__field3", ["field3"], "example", "example__field4"),
            self.fake_query_result("example__field4", ["field4"], "example","example__field5"),
            self.fake_query_result("example__field5", ["field5"], "example")
        ]
        index_metadata = Collection("index", "name")
        index_by_collection_metadata = Index(None, index_metadata.name, ["collection.name"], None)
        collection_name = "example"
        indexes = IndexService.get_indexes_from_collection_name_generator(collection_name, 2)
        names = list(map(lambda i: i.index_name, indexes))
        self.assertEqual(5, len(names))
        self.assertEqual(["example__field1", "example__field2", "example__field3", "example__field4", "example__field5"], names)
        self.assertEqual(call(index_metadata,Eq("collection.name", collection_name),index_by_collection_metadata,None,2), mock_query.call_args_list[0])
    #
    #
    # @patch('dynamoplus.service.system.system.SystemService.get_index')
    # def test_find_index_matching_fields(self, mock_get_index):
    #     expected_index = Index("1", "example", "field1")
    #     mock_get_index.side_effect = [None,None,expected_index]
    #     index = SystemService.get_index_matching_fields(["field1","field2","field3"],"example")
    #     self.assertEqual(expected_index,index)
    #     calls = [call("field1__field2__field3","example"),call("field1__field2","example"),call("field1","example")]
    #     mock_get_index.assert_has_calls(calls)
    #

    @patch('dynamoplus.v2.service.system.system_service.IndexService.get_index_by_name_and_collection_name')
    def test_find_index_matching_fields_not_found(self, mock_get_index_by_name_and_collection_name):

        mock_get_index_by_name_and_collection_name.side_effect = [None, None, None]
        index = IndexService.get_index_matching_fields(["field1", "field2", "field3"], "example")
        self.assertIsNone(index)
        calls = [call("example__field1__field2__field3", "example"), call("example__field1__field2", "example"),
                 call("example__field1", "example")]
        mock_get_index_by_name_and_collection_name.assert_has_calls(calls)


    def fake_query_result(self, index_name, conditions, collection_name, next = None):
        return QueryResult(
            [Model("index#"+index_name,"index", index_name,
                   { "name": index_name, "collection": {"name": collection_name},
                    "conditions": conditions})], next)
