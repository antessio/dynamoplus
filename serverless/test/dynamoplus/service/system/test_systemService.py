import os
import unittest
from decimal import Decimal

from dynamoplus.models.query.conditions import Eq, And, AnyMatch, Predicate, FieldMatch
from dynamoplus.models.system.aggregation.aggregation import AggregationConfiguration, AggregationType, \
    AggregationTrigger, \
    AggregationJoin, Aggregation, AggregationCount
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.system.index.index import Index, IndexConfiguration
from dynamoplus.models.system.client_authorization.client_authorization import ClientAuthorizationHttpSignature, \
    ClientAuthorizationApiKey, Scope, ScopesType

from mock import call
from unittest.mock import patch

from dynamoplus.v2.repository.repositories import Model, Repository, QueryResult, AtomicIncrement, Counter
from dynamoplus.v2.service.query_service import QueryService
from dynamoplus.v2.service.system.system_service import AuthorizationService, CollectionService, IndexService, \
    Converter
from dynamoplus.v2.service.system.system_service import AggregationConfigurationService

domain_table_name = "domain"
system_table_name = "system"


class TestSystemService(unittest.TestCase):

    def setUp(self):
        os.environ["DYNAMODB_DOMAIN_TABLE"] = domain_table_name
        os.environ["DYNAMODB_SYSTEM_TABLE"] = system_table_name

    def test_convert_aggregation_configuration(self):
        collection_name = "example"
        type = AggregationType.AVG
        on = [AggregationTrigger.INSERT, AggregationTrigger.DELETE, AggregationTrigger.UPDATE]
        target_field = "field_1"
        predicate = And([Eq("field_x", "value1"), Eq("field_y", "value2")])
        join = AggregationJoin("my_collection", "field_example_1")
        aggregation = AggregationConfiguration(collection_name, type,
                                               on,
                                               target_field, predicate,
                                               join)
        d = Converter.from_aggregation_configuration_to_dict(aggregation)
        self.maxDiff = None
        expected = {
            "name": aggregation.name,
            "collection": {
                "name": collection_name
            },
            "type": type.name,
            "aggregation": {
                "on": [AggregationTrigger.INSERT.name, AggregationTrigger.DELETE.name, AggregationTrigger.UPDATE.name],
                "target_field": target_field,
                "join": {
                    "collection_name": join.collection_name,
                    "using_field": join.using_field
                },
                "matches": {
                    "and": [
                        {
                            "eq": {
                                "field_name": "field_x",
                                "value": "value1"
                            }
                        },
                        {
                            "eq": {
                                "field_name": "field_y",
                                "value": "value2"
                            }
                        }
                    ]
                }
            }
        }
        self.assertDictEqual(d, expected)
        aggregation_result = Converter.from_dict_to_aggregation_configuration(expected)
        self.assertEqual(aggregation, aggregation_result)

    # def test_from_aggregation_configuration_to_API(self):
    #     aggregation_configuration = AggregationConfiguration("example",AggregationType.COLLECTION_COUNT,[AggregationTrigger.INSERT,AggregationTrigger.DELETE],"field1",AnyMatch(),AggregationJoin("example_2","field2"))
    #     aggregation = AggregationCount("example_count","exampel_count",40)
    #     result = Converter.from_aggregation_configuration_to_API(aggregation_configuration, aggregation)
    #     self.assertDictEqual({
    #         "aggregation": {"name": }0236617100.
    #     },result)

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
        index = Index("example", expected_conditions, IndexConfiguration.OPTIMIZE_READ, "field2.field21")
        expected_index_model = Model("index#" + expected_name, "index", expected_name, target_index)
        mock_query.return_value = QueryResult([expected_index_model], None)
        created_index = IndexService.create_index(index)
        index_name = created_index.index_name
        self.assertEqual(index_name, expected_name)
        self.assertFalse(mock_repository.called)
        self.assertFalse(mock_create.called)
        index_metadata = Collection("index", "name")
        mock_query.assert_called_once_with(index_metadata, Eq("name", expected_name),
                                           Index(index_metadata.name, ["name"], None), None, 1)

    @patch.object(QueryService, "query")
    @patch.object(Repository, "create")
    @patch.object(Repository, "__init__")
    def test_createIndexWithOrdering(self, mock_repository, mock_create, mock_query):
        expected_name = 'example__field1__field2.field21__ORDER_BY__field2.field21'
        expected_conditions = ["field1", "field2.field21"]
        target_index = {
            "name": expected_name,
            "collection": {"name": "example"},
            "conditions": expected_conditions,
            "configuration": "OPTIMIZE_READ",
            "ordering_key": "field2.field21"}
        # expected_model = Model(index_metadata, target_index)
        expected_index_model = Model("index#" + expected_name, "index", expected_name, target_index)

        mock_repository.return_value = None
        mock_create.return_value = expected_index_model
        mock_query.return_value = QueryResult([], None)
        index = Index("example", expected_conditions, IndexConfiguration.OPTIMIZE_READ, "field2.field21")
        created_index = IndexService.create_index(index)
        index_name = created_index.index_name
        self.assertEqual(index_name, expected_name)
        # self.assertEqual(call(expected_index_model), mock_create.call_args_list[0])
        calls = [call(expected_index_model),
                 call(Model("index#" + expected_name, "index#collection.name", "example", target_index))]
        mock_create.assert_has_calls(calls)

    @patch.object(QueryService, "query")
    def test_queryIndex_by_CollectionByName(self, mock_query):
        # expected_query = Query(Eq("collection.name", "example"), Collection("index", "uid"),["collection.name"])
        index_metadata = Collection("index", "name")
        index_by_collection_metadata = Index(index_metadata.name, ["collection.name"], None)
        collection_name = "example"
        mock_query.return_value = QueryResult(
            [Model("index#collection.name", "index", collection_name,
                   {"uid": "1", "name": "collection.name", "collection": {"name": collection_name},
                    "conditions": ["collection.name"]})])
        indexes, last_key = IndexService.get_index_by_collection_name(collection_name)
        self.assertEqual(1, len(indexes))
        self.assertEqual(indexes[0].index_name, "example__collection.name")
        self.assertEqual(
            call(index_metadata, Eq("collection.name", collection_name), index_by_collection_metadata, None, 20),
            mock_query.call_args_list[0])

    @patch.object(QueryService, "query")
    def test_queryIndex_by_CollectionByName_generator(self, mock_query):
        mock_query.side_effect = [
            self.fake_query_result_index("example__field1", ["field1"], "example", "example__field2"),
            self.fake_query_result_index("example__field2", ["field2"], "example", "example__field3"),
            self.fake_query_result_index("example__field3", ["field3"], "example", "example__field4"),
            self.fake_query_result_index("example__field4", ["field4"], "example", "example__field5"),
            self.fake_query_result_index("example__field5", ["field5"], "example")
        ]
        index_metadata = Collection("index", "name")
        index_by_collection_metadata = Index(index_metadata.name, ["collection.name"], None)
        collection_name = "example"
        indexes = IndexService.get_indexes_from_collection_name_generator(collection_name, 2)
        names = list(map(lambda i: i.index_name, indexes))
        self.assertEqual(5, len(names))
        self.assertEqual(
            ["example__field1", "example__field2", "example__field3", "example__field4", "example__field5"], names)
        self.assertEqual(
            call(index_metadata, Eq("collection.name", collection_name), index_by_collection_metadata, None, 2),
            mock_query.call_args_list[0])

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

    @patch.object(Repository, "create")
    @patch.object(Repository, "__init__")
    def test_create_aggregation(self, mock_repository, mock_create):
        collection_name = "example"
        type = AggregationType.AVG
        on = [AggregationTrigger.INSERT, AggregationTrigger.DELETE, AggregationTrigger.UPDATE]
        target_field = "field_1"
        predicate = And([Eq("field_x", "value1"), Eq("field_y", "value2")])
        join = AggregationJoin("my_collection", "field_example_1")
        aggregation = AggregationConfiguration(collection_name, type,
                                               on,
                                               target_field, predicate,
                                               join)
        target_agg = {
            "name": aggregation.name,
            "collection": {"name": collection_name}, "type": type.name,
            "aggregation": {
                "on": [AggregationTrigger.INSERT.name, AggregationTrigger.DELETE.name, AggregationTrigger.UPDATE.name],
                "target_field": target_field,
                "join": {"collection_name": join.collection_name, "using_field": join.using_field},
                "matches": {"and": [
                    {
                        "eq": {
                            "field_name": "field_x",
                            "value": "value1"
                        }
                    },
                    {
                        "eq": {
                            "field_name": "field_y",
                            "value": "value2"
                        }
                    }
                ]
                }
            }
        }
        expected_model = Model("aggregation_configuration#" + aggregation.name, "aggregation_configuration", aggregation.name, target_agg)
        mock_repository.return_value = None
        mock_create.return_value = expected_model

        created_aggregation = AggregationConfigurationService.create_aggregation_configuration(aggregation)
        mock_repository.assert_called_once_with(system_table_name)
        aggregation_name = created_aggregation.name
        self.assertEqual(aggregation_name, aggregation.name)
        calls = [call(expected_model),
                 call(Model("aggregation_configuration#" + aggregation.name, "aggregation_configuration#collection.name",
                            aggregation.collection_name, target_agg))]
        mock_create.assert_has_calls(calls)

    @patch.object(Repository, "get")
    @patch.object(Repository, "__init__")
    def test_get_aggregation_by_name(self, mock_repository, mock_get):
        expected_name = 'example_collection_count'
        mock_repository.return_value = None
        collection_name = "client_authorization"
        document = {"name": expected_name, "collection": {"name": "example"},
                    "type": "COLLECTION_COUNT",
                    "aggregation": {
                        "on": ["INSERT"]
                    }}
        expected_model = Model(collection_name + "#" + expected_name, collection_name, expected_name,
                               document)
        mock_get.return_value = expected_model
        result = AggregationConfigurationService.get_aggregation_configuration_by_name(expected_name)
        mock_repository.assert_called_once_with(system_table_name)
        self.assertTrue(mock_get.called_with(collection_name + "#" + expected_name, collection_name))
        self.assertEqual(expected_name, result.name)
        self.assertIsInstance(result, AggregationConfiguration)

    @patch.object(Converter, "from_dict_to_aggregation_configuration")
    @patch.object(QueryService, "query_generator")
    def test_get_aggregations_by_collection_name(self, mock_query, mock_converter):
        collection_name = "example"
        expected_results = [
            self.fake_query_result_aggregation("example_count", "example",
                                               {"name": "example_count", "collection": {"name": "example"},
                                                "type": "count", "aggregation": {"on": ""}}),
            self.fake_query_result_aggregation("example_avg_rate", "example",
                                               {"name": "example_avg_rate", "collection": {"name": "example"},
                                                "type": "avg", "aggregation": {"on": ""}}),
            self.fake_query_result_aggregation("example_sum_amount_by_foo", "example",
                                               {"name": "example_sum_amount_by_foo", "collection": {"name": "example"},
                                                "type": "sum", "aggregation": {"on": ""}})
        ]

        def fake(x, y, z):
            yield from expected_results

        mock_query.side_effect = fake
        mock_converter.return_value = AggregationConfiguration("example", AggregationType.COLLECTION_COUNT,
                                                               [AggregationTrigger.INSERT], "whatever", None, None)

        aggregation_metadata = Collection("aggregation_configuration", "name")
        index_by_collection_metadata = Index(aggregation_metadata.name, ["collection.name"])
        aggregations = AggregationConfigurationService.get_aggregation_configurations_by_collection_name_generator(collection_name)

        names = list(map(lambda a: a.name, aggregations))
        self.assertEqual(3, len(names))
        self.assertEqual(
            call(aggregation_metadata,
                 Eq("collection.name", collection_name),
                 index_by_collection_metadata),
            mock_query.call_args_list[0])
        self.assertEqual(3, mock_converter.call_count)

    @patch.object(Converter, "from_dict_to_aggregation_configuration")
    @patch.object(QueryService, "query")
    def test_get_all_aggregations(self, mock_query, mock_converter):
        expected_results = [
            self.fake_query_result_aggregation("example_count", "example",
                                               {"name": "example_count", "collection": {"name": "example"},
                                                "type": "count", "aggregation": {"on": ""}}),
            self.fake_query_result_aggregation("example_avg_rate", "example",
                                               {"name": "example_avg_rate", "collection": {"name": "example"},
                                                "type": "avg", "aggregation": {"on": ""}}),
            self.fake_query_result_aggregation("example_sum_amount_by_foo", "example",
                                               {"name": "example_sum_amount_by_foo",
                                                "collection": {"name": "example"}, "type": "sum",
                                                "aggregation": {"on": ""}})
        ]

        mock_query.return_value = QueryResult(expected_results, None)
        mock_converter.return_value = AggregationConfiguration("example", AggregationType.COLLECTION_COUNT,
                                                               [AggregationTrigger.INSERT], "whatever", None, None)

        aggregation_metadata = Collection("aggregation_configuration", "name")
        aggregations, last_key = AggregationConfigurationService.get_all_aggregation_configurations(20, None)

        names = list(map(lambda a: a.name, aggregations))
        self.assertEqual(3, len(names))
        self.assertEqual(
            call(aggregation_metadata,
                 AnyMatch(),
                 None,
                 20,
                 None),
            mock_query.call_args_list[0])
        self.assertEqual(3, mock_converter.call_count)

    def fake_query_result_index(self, index_name, conditions, collection_name, next=None):
        return QueryResult(
            [Model("index#" + index_name, "index", index_name,
                   {"name": index_name, "collection": {"name": collection_name},
                    "conditions": conditions})], next)

    def fake_query_result_aggregation(self, aggregation_name, collection_name, d: dict):
        return Model("aggregation_configuration#" + aggregation_name, "aggregation_configuration", aggregation_name, d)
