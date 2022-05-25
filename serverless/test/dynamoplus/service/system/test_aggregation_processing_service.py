import os
import unittest
from unittest.mock import patch

from mock import call

from dynamoplus.models.query.conditions import Eq
from dynamoplus.models.system.aggregation.aggregation import AggregationConfiguration, AggregationType, \
    AggregationTrigger, \
    AggregationCount, AggregationAvg, AggregationSum
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.v2.service.system.aggregation_service import AggregationProcessingService
from dynamoplus.v2.service.system.system_service import AggregationService

domain_table_name = "domain"
system_table_name = "system"


class TestAggregationProcessingService(unittest.TestCase):




    def setUp(self):
        os.environ["DYNAMODB_DOMAIN_TABLE"] = domain_table_name
        os.environ["DYNAMODB_SYSTEM_TABLE"] = system_table_name

    def test_not_matching_trigger(self):
        aggregation_configuration = AggregationConfiguration("example", AggregationType.COLLECTION_COUNT,
                                                             [AggregationTrigger.INSERT, AggregationTrigger.DELETE],
                                                             None, None, None)
        document = {
            "name": "whatever"
        }
        example_collection = Collection("example", "id")
        result = AggregationProcessingService.execute_aggregation(aggregation_configuration, example_collection,
                                                                  document,
                                                                  document)
        self.assertEqual(result,None)

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
    def test_collection_avg_aggregation_increase(self, mock_get_aggregations_by_name_generator,
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

    @patch.object(AggregationService, "increment")
    @patch.object(AggregationService, "updateAggregation")
    @patch.object(AggregationService, "get_aggregation_by_name")
    @patch.object(AggregationService, "get_aggregations_by_name_generator")
    def test_collection_avg_aggregation_decrease(self, mock_get_aggregations_by_name_generator,
                                                 mock_get_aggregation_by_name,
                                                 mock_update_aggregation,
                                                 mock_increment):
        target_field = "attribute"
        aggregation_configuration = AggregationConfiguration("example", AggregationType.AVG,
                                                             [AggregationTrigger.INSERT, AggregationTrigger.UPDATE,
                                                              AggregationTrigger.DELETE],
                                                             target_field, None, None)
        existing_avg = 4.33
        existing_sum = 13
        existing_count = 3
        value = 6
        expected_new_count = existing_count - 1
        expected_new_sum = existing_sum - value
        expected_new_avg = (expected_new_sum) / (expected_new_count)
        expected_aggregation = AggregationAvg(aggregation_configuration.name, aggregation_configuration.name,
                                              expected_new_avg)
        old_record = {
            "name": "whatever",
            target_field: value
        }
        example_collection = Collection("example", "id")
        expected_existing_aggregtion_count = AggregationCount("count_" + aggregation_configuration.name,
                                                              aggregation_configuration.name, existing_count)
        expected_existing_aggregation_sum = AggregationSum("sum_" + aggregation_configuration.name,
                                                           aggregation_configuration.name, existing_sum)
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
            AggregationCount("count_" + aggregation_configuration.name, aggregation_configuration.name,
                             expected_new_sum - 1),
            AggregationSum("sum_" + aggregation_configuration.name, aggregation_configuration.name, expected_new_sum)
        ]
        mock_update_aggregation.return_value = AggregationAvg(aggregation_configuration.name,
                                                              aggregation_configuration.name, expected_new_avg)

        result = AggregationProcessingService.execute_aggregation(aggregation_configuration, example_collection,
                                                                  None,
                                                                  old_record)
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
            call(expected_existing_aggregtion_count, "count", -1),
            call(expected_existing_aggregation_sum, "sum", value*-1),
        ])

    @patch.object(AggregationService, "create_aggregation")
    @patch.object(AggregationService, "get_aggregation_by_name")
    def test_collection_sum_aggregation_not_found(self, mock_get_aggregation_by_name, mock_createAggregation):
        target_field = "attribute"
        value = 13
        aggregation_configuration = AggregationConfiguration("example", AggregationType.SUM,
                                                             [AggregationTrigger.INSERT, AggregationTrigger.DELETE],
                                                             target_field, None, None)
        expected_aggregation = AggregationSum(aggregation_configuration.name, aggregation_configuration.name, value)
        document = {
            "name": "whatever",
            target_field: value

        }
        example_collection = Collection("example", "id")
        mock_get_aggregation_by_name.return_value = None
        mock_createAggregation.return_value = expected_aggregation
        result = AggregationProcessingService.execute_aggregation(aggregation_configuration, example_collection,
                                                                  document,
                                                                  None)
        self.assertEqual(result, expected_aggregation)
        self.assertEqual(call(aggregation_configuration.name), mock_get_aggregation_by_name.call_args_list[0])
        self.assertEqual(call(AggregationSum(aggregation_configuration.name, aggregation_configuration.name, value)),
                         mock_createAggregation.call_args_list[0])

    @patch.object(AggregationService, "increment")
    @patch.object(AggregationService, "get_aggregation_by_name")
    def test_collection_sum_aggregation_increment(self, mock_get_aggregation_by_name, mock_increment):
        target_field = "attribute"
        value = 13
        aggregation_configuration = AggregationConfiguration("example", AggregationType.SUM,
                                                             [AggregationTrigger.INSERT, AggregationTrigger.DELETE],
                                                             target_field, None, None)
        existing_sum = 20
        document = {
            "name": "whatever",
            target_field: value
        }
        example_collection = Collection("example", "id")
        expected_existing_aggregation_sum = AggregationSum(aggregation_configuration.name,
                                                               aggregation_configuration.name, existing_sum)
        mock_get_aggregation_by_name.return_value = expected_existing_aggregation_sum
        mock_increment.return_value = expected_existing_aggregation_sum
        existing_sum = expected_existing_aggregation_sum.sum
        result = AggregationProcessingService.execute_aggregation(aggregation_configuration, example_collection,
                                                                  document,
                                                                  None)

        self.assertEqual(result.sum, existing_sum + value)
        self.assertEqual(call(aggregation_configuration.name), mock_get_aggregation_by_name.call_args_list[0])
        self.assertEqual(call(expected_existing_aggregation_sum,"sum",value),
                         mock_increment.call_args_list[0])

    @patch.object(AggregationService, "increment")
    @patch.object(AggregationService, "get_aggregation_by_name")
    def test_collection_sum_aggregation_decrement(self, mock_get_aggregation_by_name, mock_increment):
        target_field = "attribute"
        value = 13
        aggregation_configuration = AggregationConfiguration("example", AggregationType.SUM,
                                                             [AggregationTrigger.INSERT, AggregationTrigger.DELETE],
                                                             target_field, None, None)
        existing_sum = 20
        document = {
            "name": "whatever",
            target_field: value
        }
        example_collection = Collection("example", "id")
        expected_existing_aggregation_sum = AggregationSum(aggregation_configuration.name,
                                                           aggregation_configuration.name, existing_sum)
        mock_get_aggregation_by_name.return_value = expected_existing_aggregation_sum
        mock_increment.return_value = expected_existing_aggregation_sum
        existing_sum = expected_existing_aggregation_sum.sum
        result = AggregationProcessingService.execute_aggregation(aggregation_configuration, example_collection,
                                                                  None,
                                                                  document)

        self.assertEqual(result.sum, existing_sum - value)
        self.assertEqual(call(aggregation_configuration.name), mock_get_aggregation_by_name.call_args_list[0])
        self.assertEqual(call(expected_existing_aggregation_sum, "sum", value*-1),
                         mock_increment.call_args_list[0])

    def test_not_matching_predicate(self):
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
        result=AggregationProcessingService.execute_aggregation(aggregation, example_collection, new_record, old_document)
        self.assertEqual(result,None)

