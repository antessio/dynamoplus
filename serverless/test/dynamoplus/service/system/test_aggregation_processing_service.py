import os
import unittest
from decimal import Decimal

from dynamoplus.models.query.conditions import Eq, And
from dynamoplus.models.system.aggregation.aggregation import AggregationConfiguration, AggregationType, \
    AggregationTrigger, \
    AggregationJoin, Aggregation, AggregationCount, AggregationAvg, AggregationSum
from dynamoplus.models.system.collection.collection import Collection

from mock import call
from unittest.mock import patch

from dynamoplus.v2.repository.repositories import Model, Repository, QueryResult, AtomicIncrement, Counter
from dynamoplus.v2.service.system.aggregation_service import AggregationProcessingService
from dynamoplus.v2.service.system.system_service import AggregationService, AggregationConfigurationService

domain_table_name = "domain"
system_table_name = "system"


class TestAggregationProcessingService(unittest.TestCase):




    def setUp(self):
        os.environ["DYNAMODB_DOMAIN_TABLE"] = domain_table_name
        os.environ["DYNAMODB_SYSTEM_TABLE"] = system_table_name

    @patch.object(AggregationService, "create_aggregation")
    @patch.object(AggregationService, "get_aggregation_by_name")
    def test_collection_count_aggregation_not_found(self, mock_get_aggregation_by_name, mock_createAggregation):
        aggregation_configuration = AggregationConfiguration("example", AggregationType.COLLECTION_COUNT,
                                                             [AggregationTrigger.INSERT, AggregationTrigger.DELETE],
                                                             None, None, None)
        expected_aggregation = AggregationCount(aggregation_configuration.name, aggregation_configuration.name,1)
        document = {
            "name": "whatever"
        }
        example_collection = Collection("example", "id")
        mock_get_aggregation_by_name.return_value = None
        mock_createAggregation.return_value = expected_aggregation
        result = AggregationProcessingService.execute_aggregation(aggregation_configuration, example_collection, document,
                                                         None)
        self.assertEqual(result, expected_aggregation)
        self.assertEqual(call(aggregation_configuration.name), mock_get_aggregation_by_name.call_args_list[0])
        self.assertEqual(call(AggregationCount(aggregation_configuration.name, aggregation_configuration.name, 1)),
                         mock_createAggregation.call_args_list[0])

    @patch.object(AggregationService, "increment_count")
    @patch.object(AggregationService, "get_aggregation_by_name")
    def test_collection_count_aggregation_increment(self, mock_get_aggregation_by_name, mock_increment_count):

        aggregation_configuration = AggregationConfiguration("example", AggregationType.COLLECTION_COUNT,
                                                             [AggregationTrigger.INSERT, AggregationTrigger.DELETE],
                                                             None, None, None)
        document = {
            "name": "whatever"
        }
        example_collection = Collection("example", "id")
        expected_existing_aggregation_count = AggregationCount(aggregation_configuration.name, aggregation_configuration.name, 1)
        mock_get_aggregation_by_name.return_value = expected_existing_aggregation_count
        mock_increment_count.return_value = expected_existing_aggregation_count
        existing_count = expected_existing_aggregation_count.count
        result = AggregationProcessingService.execute_aggregation(aggregation_configuration, example_collection, document,
                                                         None)

        self.assertEqual(result.count, existing_count + 1)
        self.assertEqual(call(aggregation_configuration.name), mock_get_aggregation_by_name.call_args_list[0])
        self.assertEqual(call(expected_existing_aggregation_count),
                         mock_increment_count.call_args_list[0])

    @patch.object(AggregationService, "decrement_count")
    @patch.object(AggregationService, "get_aggregation_by_name")
    def test_collection_count_aggregation_decrement(self, mock_get_aggregation_by_name, mock_decrement_count):
        aggregation_configuration = AggregationConfiguration("example", AggregationType.COLLECTION_COUNT,
                                                             [AggregationTrigger.INSERT, AggregationTrigger.DELETE],
                                                             None, None, None)
        document = {
            "name": "whatever"
        }
        example_collection = Collection("example", "id")
        expected_existing_aggregation_count = AggregationCount(aggregation_configuration.name,
                                                               aggregation_configuration.name, 10)
        expected_existing_count = expected_existing_aggregation_count.count
        mock_get_aggregation_by_name.return_value = expected_existing_aggregation_count
        mock_decrement_count.return_value = expected_existing_aggregation_count
        result = AggregationProcessingService.execute_aggregation(aggregation_configuration, example_collection,
                                                                  None,
                                                                  document)
        self.assertEqual(result.count, expected_existing_count-1)
        self.assertEqual(call(aggregation_configuration.name), mock_get_aggregation_by_name.call_args_list[0])
        self.assertEqual(call(expected_existing_aggregation_count),
                         mock_decrement_count.call_args_list[0])

    @patch.object(AggregationService, "create_aggregation")
    @patch.object(AggregationService, "get_aggregation_by_name")
    @patch.object(AggregationService, "get_aggregations_by_name_generator")
    def test_collection_avg_aggregation_not_found(self,mock_get_aggregations_by_name_generator, mock_get_aggregation_by_name, mock_createAggregation):
        target_field = "attribute"
        aggregation_configuration = AggregationConfiguration("example", AggregationType.AVG,
                                                             [AggregationTrigger.INSERT, AggregationTrigger.UPDATE, AggregationTrigger.DELETE],
                                                             target_field, None, None)
        expected_value = 7.0
        expected_aggregation = AggregationAvg(aggregation_configuration.name, aggregation_configuration.name, expected_value)
        document = {
            "name": "whatever",
            target_field: expected_value
        }
        example_collection = Collection("example", "id")
        mock_get_aggregations_by_name_generator.return_value = []
        mock_get_aggregation_by_name.return_value = None
        mock_createAggregation.return_value = expected_aggregation
        result = AggregationProcessingService.execute_aggregation(aggregation_configuration, example_collection,
                                                                  document,
                                                                  None)
        self.assertEqual(result, expected_aggregation)
        mock_get_aggregation_by_name.assert_not_called()
        mock_createAggregation.assert_has_calls([
            call(AggregationCount("count_" + aggregation_configuration.name, aggregation_configuration.name, 1)),
            call(AggregationSum("sum_" + aggregation_configuration.name, aggregation_configuration.name, expected_value)),
            call(AggregationAvg(aggregation_configuration.name, aggregation_configuration.name, expected_value))
        ])

    @patch.object(AggregationService, "increment")
    @patch.object(AggregationService, "updateAggregation")
    @patch.object(AggregationService, "get_aggregation_by_name")
    @patch.object(AggregationService, "get_aggregations_by_name_generator")
    def test_collection_avg_aggregation_increae(self, mock_get_aggregations_by_name_generator,
                                                mock_get_aggregation_by_name, mock_update_aggregation, mock_increment):
        target_field = "attribute"
        aggregation_configuration = AggregationConfiguration("example", AggregationType.AVG,
                                                             [AggregationTrigger.INSERT, AggregationTrigger.UPDATE,
                                                              AggregationTrigger.DELETE],
                                                             target_field, None, None)
        existing_avg = 7.0
        existing_sum = 14
        existing_count = 2
        value = 3
        expected_new_count = existing_count + 1
        expected_new_sum = existing_sum + value
        expected_new_avg = (expected_new_sum) / (expected_new_count)
        expected_aggregation = AggregationAvg(aggregation_configuration.name, aggregation_configuration.name,
                                              expected_new_avg)
        new_record = {
            "name": "whatever",
            target_field: value
        }
        example_collection = Collection("example", "id")
        expected_existing_aggregtion_count = AggregationCount("count_" + aggregation_configuration.name, aggregation_configuration.name, existing_count)
        expected_existing_aggregation_sum = AggregationSum("sum_" + aggregation_configuration.name, aggregation_configuration.name, existing_sum)
        mock_get_aggregations_by_name_generator.return_value = [
            AggregationAvg(aggregation_configuration.name, aggregation_configuration.name,
                           existing_avg),
            expected_existing_aggregation_sum,
            expected_existing_aggregtion_count
        ]
        mock_get_aggregation_by_name.side_effect = [
            expected_existing_aggregtion_count,
            expected_existing_aggregation_sum,
            AggregationAvg(aggregation_configuration.name, aggregation_configuration.name,
                           existing_avg)
        ]
        mock_increment.side_effect = [
            AggregationCount("count_" + aggregation_configuration.name, aggregation_configuration.name, expected_new_sum-1),
            AggregationSum("sum_" + aggregation_configuration.name, aggregation_configuration.name, expected_new_sum)
        ]
        mock_update_aggregation.return_value = AggregationAvg(aggregation_configuration.name, aggregation_configuration.name, expected_new_avg)

        result = AggregationProcessingService.execute_aggregation(aggregation_configuration, example_collection,
                                                                  new_record,
                                                                  None)
        self.assertEqual(result, expected_aggregation)
        mock_get_aggregation_by_name.assert_has_calls([
            call('example_avg_attribute'),
            call('sum_example_avg_attribute'),
            call('count_example_avg_attribute')
        ])
        mock_update_aggregation.assert_has_calls([
            call(AggregationAvg(aggregation_configuration.name, aggregation_configuration.name, expected_new_avg))
        ])
        mock_increment.assert_has_calls([
            call(expected_existing_aggregtion_count,"count",1),
            call(expected_existing_aggregation_sum, "sum", value),
        ])

    @unittest.skip("reason for skipping")
    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_collection_avg_insert(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None

        aggregation = AggregationConfiguration("example", AggregationType.AVG,
                                               [AggregationTrigger.INSERT, AggregationTrigger.DELETE, AggregationTrigger.UPDATE],
                                  "rate", None, None)
        document = {
            "id": 1,
            "name": "whatever",
            "rate": 4
        }
        example_collection = Collection("example", "id")
        AggregationProcessingService.execute_aggregation(aggregation, example_collection, document, None)
        self.assertTrue(mock_repository.called)
        self.assertEqual(
            call(AtomicIncrement("collection#example", "collection",
                                 [Counter("rate_count", Decimal(1)), Counter("rate_sum", Decimal(4))]
                                 )),
            mock_increment_counter.call_args_list[0])

    @unittest.skip("reason for skipping")
    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_collection_avg_delete(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None
        example_collection = Collection("example", "id")
        aggregation = AggregationConfiguration("example", AggregationType.AVG,
                                               [AggregationTrigger.INSERT, AggregationTrigger.DELETE, AggregationTrigger.UPDATE],
                                  "rate", None, None)
        document = {
            "id": 1,
            "name": "whatever",
            "rate": 4
        }
        AggregationProcessingService.execute_aggregation(aggregation, example_collection, None, document)
        self.assertTrue(mock_repository.called)
        expected_increment = AtomicIncrement("collection#example", "collection",
                                             [Counter("rate_count", Decimal(1), False),
                                              Counter("rate_sum", Decimal(4), False)])
        self.assertEqual(
            call(expected_increment),
            mock_increment_counter.call_args_list[0])

    @unittest.skip("reason for skipping")
    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_collection_avg_update_decrease(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None
        example_collection = Collection("example", "id")
        aggregation = AggregationConfiguration("example", AggregationType.AVG,
                                               [AggregationTrigger.INSERT, AggregationTrigger.DELETE, AggregationTrigger.UPDATE],
                                  "rate", None, None)
        old_document = {
            "id": 1,
            "name": "whatever",
            "rate": 4
        }
        new_record = {
            "id": 1,
            "name": "whatever",
            "rate": 2
        }
        AggregationProcessingService.execute_aggregation(aggregation, example_collection, new_record, old_document)
        self.assertTrue(mock_repository.called)
        self.assertEqual(
            call(AtomicIncrement("collection#example", "collection",
                                 [
                                     Counter("rate_sum", Decimal(2), False)
                                 ]
                                 )),
            mock_increment_counter.call_args_list[0])

    @unittest.skip("reason for skipping")
    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_collection_avg_update_increase(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None
        example_collection = Collection("example", "id")
        aggregation = AggregationConfiguration("example", AggregationType.AVG,
                                               [AggregationTrigger.INSERT, AggregationTrigger.DELETE, AggregationTrigger.UPDATE],
                                  "rate", None, None)
        old_document = {
            "id": 1,
            "name": "whatever",
            "rate": 2
        }
        new_record = {
            "id": 1,
            "name": "whatever",
            "rate": 6
        }
        AggregationProcessingService.execute_aggregation(aggregation, example_collection, new_record, old_document)
        self.assertTrue(mock_repository.called)
        self.assertEqual(
            call(AtomicIncrement("collection#example", "collection",
                                 [
                                     Counter("rate_sum", Decimal(4))
                                 ]
                                 )),
            mock_increment_counter.call_args_list[0])

    @unittest.skip("reason for skipping")
    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_not_matching_predicate(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None
        example_collection = Collection("example", "id")
        aggregation = AggregationConfiguration("example", AggregationType.AVG,
                                               [AggregationTrigger.INSERT, AggregationTrigger.DELETE, AggregationTrigger.UPDATE],
                                  "rate", Eq("name", "example-name"), None)
        old_document = {
            "id": 1,
            "name": "whatever",
            "rate": 2
        }
        new_record = {
            "id": 1,
            "name": "whatever",
            "rate": 6
        }
        AggregationProcessingService.execute_aggregation(aggregation, example_collection, new_record, old_document)
        self.assertFalse(mock_repository.called)
        self.assertFalse(mock_increment_counter.called)

    # @patch('dynamoplus.v2.service.system.system_service.IndexService.get_index_by_name_and_collection_name')
    # @patch.object(Repository, "increment_counter")
    # @patch.object(Repository, "__init__")
    # def test_collection_avg_join(self, mock_repository, mock_increment_counter):
    #     mock_repository.return_value = None
    #     example_collection = Collection("example", "id")
    #     aggregation = Aggregation("restaurant", AggregationType.AVG_JOIN,
    #                               [AggregationTrigger.INSERT, AggregationTrigger.DELETE, AggregationTrigger.UPDATE],
    #                               "rate", None, AggregationJoin("review","restaurant_id"))
    #     example_collection = Collection("example", "id")
    #     document = {
    #         "id": 1,
    #         "name": "whatever",
    #         "rate": 4
    #     }
    #     AggregationService.execute_aggregation(aggregation, example_collection, document, None)
    #     self.assertTrue(mock_repository.called)
    #     self.assertEqual(
    #         call(AtomicIncrement("collection#example", "collection",
    #                              [Counter("rate_count", Decimal(1)), Counter("rate_sum", Decimal(4))]
    #                              )),
    #         mock_increment_counter.call_args_list[0])

    def fake_query_result(self, index_name, conditions, collection_name, next=None):
        return QueryResult(
            [Model("index#" + index_name, "index", index_name,
                   {"name": index_name, "collection": {"name": collection_name},
                    "conditions": conditions})], next)
