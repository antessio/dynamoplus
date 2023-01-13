import os
import unittest
from unittest.mock import patch

from mock import call

from dynamoplus.dynamo_plus_v2 import Dynamoplus
from dynamoplus.models.query.conditions import Eq
from dynamoplus.models.system.aggregation.aggregation import AggregationConfiguration, AggregationType, Aggregation, \
    AggregationTrigger, AggregationJoin, AggregationAvg
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.system.index.index import Index
from aws.dynamodb.dynamodbdao import QueryResult, DynamoDBModel
from dynamoplus.v2.service.query_service import QueryService
from dynamoplus.v2.service.system.system_service import CollectionService, IndexService, \
    AggregationConfigurationService, AggregationService, Converter
from test.common_test_utils import random_enum, random_aggregation_configuration_API_data


class TestDynamoPlusHandler(unittest.TestCase):

    under_test: Dynamoplus

    def setUp(self):
        self.under_test = Dynamoplus()
        os.environ['ENTITIES'] = 'index,collection,client_authorization'
        os.environ["DYNAMODB_DOMAIN_TABLE"] = "example-domain"
        os.environ["DYNAMODB_SYSTEM_TABLE"] = "example-system"

    def tearDown(self):
        del os.environ['ENTITIES']
        del os.environ["DYNAMODB_DOMAIN_TABLE"]
        del os.environ["DYNAMODB_SYSTEM_TABLE"]

    # @patch.object(AggregationService, "get_aggregation_by_name")
    # @patch.object(AggregationConfigurationService, "get_aggregation_configurations_by_collection_name")
    # def test_aggragation_configuration(self, mock_get_aggreagtion_configurations,mock_get_aggregation):
    #     collection_name = "books"
    #     last_key = "last_key"
    #     limit = 20
    #     expected_aggregation = AggregationConfiguration(collection_name, AggregationType.COLLECTION_COUNT, [AggregationTrigger.INSERT], None, None,
    #                                              None)
    #     mock_get_aggreagtion_configurations.return_value = [expected_aggregation], None
    #     mock_get_aggregation.return_value = Aggregation("test","test")
    #
    #     documents,last_key = get_aggregation_configurations(collection_name, last_key, limit)
    #
    #     self.assertEqual(len(documents),1)
    #     self.assertDictEqual(documents[0],{
    #         "collection":{
    #             "name": "books"
    #         },
    #         "type": "COLLECTION_COUNT",
    #         "configuration":{"on":["INSERT"]},
    #         "name": "books_collection_count",
    #         "aggregation": {
    #             "name": "test"
    #         }
    #     })
    #
    #
    #
    #
    # @patch.object(CollectionService, "get_collection")
    # def test_get_collection(self,  mock_get_system_collection):
    #     mock_get_system_collection.return_value = Collection("example", "id", "ordering")
    #     collection_metadata = get("collection", "example")
    #     self.assertDictEqual(collection_metadata,
    #                          {'id_key': "id", 'name': "example", 'ordering_key': "ordering", 'auto_generated_id': False})
    #     self.assertTrue(mock_get_system_collection.called_with("example"))
    #
    # @patch.object(AggregationConfigurationService, "get_aggregation_configuration_by_name")
    # def test_get_aggegation_configuration(self, mock_get_aggregation_configuration_by_name):
    #     expected_name = "example_count"
    #     expected_collection_name = "example"
    #     expected_aggregation_configuration = AggregationConfiguration(expected_collection_name, AggregationType.COLLECTION_COUNT,
    #                                              [AggregationTrigger.INSERT, AggregationTrigger.UPDATE,
    #                                               AggregationTrigger.DELETE], "attribute_1", Eq("field_1", "x"),
    #                                              AggregationJoin("example_2", "attribute_2"))
    #     mock_get_aggregation_configuration_by_name.return_value = expected_aggregation_configuration
    #     result = get("aggregation_configuration", expected_name)
    #     self.assertDictEqual(result,
    #                          {
    #                              "name": "example_field_1_x_collection_count_attribute_1by_example_2",
    #                              "collection":{
    #                                  "name": "example"
    #                              },
    #                              "type": expected_aggregation_configuration.type.name,
    #                              "configuration":{
    #                                  "join": {"collection_name": "example_2","using_field": "attribute_2"},
    #                                  "matches":{"eq":{"field_name": "field_1", "value":"x"}},
    #                                  "on":["INSERT","UPDATE","DELETE"],
    #                                  "target_field": expected_aggregation_configuration.target_field
    #                              }
    #                          })
    #     self.assertTrue(mock_get_aggregation_configuration_by_name.called_with("example"))
    #
    # @patch.object(Converter,"from_aggregation_to_API")
    # @patch.object(AggregationService, "get_aggregation_by_name")
    # def test_get_aggegation(self, mock_get_aggregation_by_name, mock_from_aggregation_to_API):
    #     expected_name = "review___api_key_test_avg_rate"
    #
    #     expected_aggregation_configuration = AggregationAvg("review___api_key_test_avg_rate","review___api_key_test_avg_rate",7.555555555555555)
    #     mock_get_aggregation_by_name.return_value = expected_aggregation_configuration
    #     expected_result = {
    #         "name": "review___api_key_test_avg_rate",
    #         "type": "AVG",
    #         "payload": {
    #             "avg": 7.555555555555555
    #         }
    #     }
    #     mock_from_aggregation_to_API.return_value = expected_result
    #     result = get("aggregation", expected_name)
    #     self.assertDictEqual(result,expected_result)
    #     self.assertTrue(mock_get_aggregation_by_name.called_with(expected_name))
    #     self.assertTrue(mock_from_aggregation_to_API.called_with(expected_aggregation_configuration))
    #
    #
    # @patch.object(QueryService, "query")
    # @patch.object(IndexService, "get_index_matching_fields")
    # @patch.object(CollectionService, "get_collection")
    # def test_get_documents_by_index(self, mock_get_collection,mock_get_index_matching_fields,mock_query):
    #     expected_collection = Collection("example", "id", "ordering")
    #     mock_get_collection.return_value = expected_collection
    #     expected_index = Index("example", ["attribute1"])
    #     expected_predicate = Eq("attribute1","1")
    #     mock_get_index_matching_fields.return_value=expected_index
    #     expected_documents = [
    #         DynamoDBModel(None, None, None, {"id": "1", "attribute1": "1"}),
    #         DynamoDBModel(None, None, None, {"id": "2", "attribute1": "1"})
    #     ]
    #     mock_query.return_value = QueryResult(expected_documents, None)
    #     documents = query("example", {"matches": {"eq":{"field_name":"attribute1", "value":"1"}}})
    #     self.assertEqual(len(documents), len(expected_documents))
    #     self.assertTrue(mock_get_collection.called_with("example"))
    #     self.assertTrue(mock_get_index_matching_fields.called_with("attribute1"))
    #     self.assertEqual(call(expected_collection,expected_predicate,expected_index, None, None), mock_query.call_args_list[0])
    #
    # @patch.object(Converter,"from_collection_to_API")
    # @patch.object(CollectionService,"get_all_collections")
    # def test_get_all_collections(self,mock_get_all_collections,mock_converter_to_API):
    #     expected_collections = [Collection("example", "id"), Collection("example_2", "id")]
    #     last_key="example_2"
    #     mock_get_all_collections.return_value=[expected_collections,last_key]
    #     expected_data = [
    #         {"name": "example", "id_key": "id"},
    #         {"name": "example_2", "id_key": "id"}
    #     ]
    #     mock_converter_to_API.side_effect=expected_data
    #     result,last_key=get_all("collection","example_0",20)
    #
    #     self.assertCountEqual(result,expected_data)
    #     mock_get_all_collections.assert_has_calls([call(20,"example_0")])
    #     mock_converter_to_API.assert_has_calls(
    #         [call(expected_collections[0]),
    #         call(expected_collections[1])]
    #     )
    #
    #
    # @patch.object(Converter,"from_collection_to_API")
    # @patch.object(CollectionService,"get_all_collections")
    # def test_get_all_collections(self,mock_get_all_collections,mock_converter_to_API):
    #     expected_collections = [Collection("example", "id"), Collection("example_2", "id")]
    #     last_key="example_2"
    #     mock_get_all_collections.return_value=[expected_collections,last_key]
    #     expected_data = [
    #         {"name": "example", "id_key": "id"},
    #         {"name": "example_2", "id_key": "id"}
    #     ]
    #     mock_converter_to_API.side_effect=expected_data
    #     result,last_key=get_all("collection","example_0",20)
    #
    #     self.assertCountEqual(result,expected_data)
    #     mock_get_all_collections.assert_has_calls([call(20,"example_0")])
    #     mock_converter_to_API.assert_has_calls(
    #         [call(expected_collections[0]),
    #         call(expected_collections[1])]
    #     )
    #
    # @patch.object(Converter, "from_aggregation_configuration_to_API")
    # @patch.object(AggregationService, 'get_aggregation_by_name')
    # @patch.object(AggregationConfigurationService, 'get_all_aggregation_configurations')
    # @unittest.skip("temporary skipping")
    # def test_get_all_aggregation_configurations(self, mock_get_all_aggregation_configurations,
    #                                             mock_get_aggregation_by_name,
    #                                             mock_from_aggregation_configuration_to_API):
    #     starting_from = '12345'
    #     last_key = '12318191'
    #     expected_aggregation_configurations = [
    #         AggregationConfiguration('example',
    #                                  random_enum(AggregationType),
    #                                  [random_enum(AggregationTrigger)],
    #                                  "field_1",
    #                                  Eq("field_1","x"),
    #                                  AggregationJoin('example_2','field_2')),
    #         AggregationConfiguration('example_2',
    #                                  random_enum(AggregationType),
    #                                  [random_enum(AggregationTrigger)],
    #                                  "field_11",
    #                                  Eq("field_21", "x"),
    #                                  AggregationJoin('example_3', 'field_X')),
    #         AggregationConfiguration('example_3',
    #                                  random_enum(AggregationType),
    #                                  [random_enum(AggregationTrigger)],
    #                                  "field_11",
    #                                  Eq("field_2x", "1"),
    #                                  AggregationJoin('example_31', 'field_1X'))
    #     ]
    #     limit = 20
    #     mock_get_all_aggregation_configurations.return_value = [expected_aggregation_configurations,last_key]
    #     expected_data = [random_aggregation_configuration_API_data() for x in expected_aggregation_configurations]
    #     mock_get_aggregation_by_name.return_value = Aggregation('example','example')
    #     mock_from_aggregation_configuration_to_API.side_effect = expected_data
    #     result, result_last_key = get_all('aggregation_configuration',starting_from,limit)
    #     self.assertDictEqual(result,expected_data)
