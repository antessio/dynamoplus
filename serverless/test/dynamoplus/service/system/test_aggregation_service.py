import os
import unittest
from decimal import Decimal

from dynamoplus.models.query.conditions import Eq, And
from dynamoplus.models.system.aggregation.aggregation import Aggregation, AggregationType, AggregationTrigger, \
    AggregationJoin
from dynamoplus.models.system.collection.collection import Collection

from mock import call
from unittest.mock import patch

from dynamoplus.v2.repository.repositories import Model, Repository, QueryResult, AtomicIncrement, Counter
from dynamoplus.v2.service.system.aggregation_service import AggregationService

domain_table_name = "domain"
system_table_name = "system"


class TestAggregationService(unittest.TestCase):

    def setUp(self):
        os.environ["DYNAMODB_DOMAIN_TABLE"] = domain_table_name
        os.environ["DYNAMODB_SYSTEM_TABLE"] = system_table_name

    @patch.object(Repository, "create")
    @patch.object(Repository, "__init__")
    def test_create_aggregation(self, mock_repository, mock_create):
        collection_name = "example"
        type = AggregationType.AVG
        on = [AggregationTrigger.INSERT, AggregationTrigger.DELETE, AggregationTrigger.UPDATE]
        target_field = "field_1"
        predicate = And([Eq("field_x", "value1"), Eq("field_y", "value2")])
        join = AggregationJoin("my_collection", "field_example_1")
        aggregation = Aggregation(collection_name, type,
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
        expected_model = Model("aggregation#" + aggregation.name, "aggregation", aggregation.name, target_agg)
        mock_repository.return_value = None
        mock_create.return_value = expected_model

        created_aggregation = AggregationService.create_aggregation(aggregation)
        mock_repository.assert_called_once_with(system_table_name)
        aggregation_name = created_aggregation.name
        self.assertEqual(aggregation_name, aggregation.name)
        calls = [call(expected_model),
                 call(Model("aggregation#" + aggregation.name, "aggregation#collection.name",
                            aggregation.collection_name, target_agg))]
        mock_create.assert_has_calls(calls)

    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_collection_count_aggregation(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None
        aggregation = Aggregation("example", AggregationType.COLLECTION_COUNT,
                                  [AggregationTrigger.INSERT, AggregationTrigger.DELETE], None, None, None)
        document = {
            "name": "whatever"
        }
        example_collection = Collection("example", "id")
        AggregationService.execute_aggregation(aggregation, example_collection, document, None)
        self.assertTrue(mock_repository.called)
        self.assertTrue(mock_increment_counter.called)
        self.assertEqual(call(AtomicIncrement("collection#example", "collection", [Counter("count", Decimal(1))])),
                         mock_increment_counter.call_args_list[0])

    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_collection_count_aggregation_remove(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None
        aggregation = Aggregation("example", AggregationType.COLLECTION_COUNT,
                                  [AggregationTrigger.INSERT, AggregationTrigger.DELETE], None, None, None)
        document = {
            "name": "whatever"
        }
        example_collection = Collection("example", "id")
        AggregationService.execute_aggregation(aggregation, example_collection, None, document)
        self.assertTrue(mock_repository.called)
        self.assertEqual(
            call(AtomicIncrement("collection#example", "collection", [Counter("count", Decimal(1), False)])),
            mock_increment_counter.call_args_list[0])

    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_collection_avg_insert(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None

        aggregation = Aggregation("example", AggregationType.AVG,
                                  [AggregationTrigger.INSERT, AggregationTrigger.DELETE, AggregationTrigger.UPDATE],
                                  "rate", None, None)
        document = {
            "id": 1,
            "name": "whatever",
            "rate": 4
        }
        example_collection = Collection("example", "id")
        AggregationService.execute_aggregation(aggregation, example_collection, document, None)
        self.assertTrue(mock_repository.called)
        self.assertEqual(
            call(AtomicIncrement("collection#example", "collection",
                                 [Counter("rate_count", Decimal(1)), Counter("rate_sum", Decimal(4))]
                                 )),
            mock_increment_counter.call_args_list[0])

    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_collection_avg_delete(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None
        example_collection = Collection("example", "id")
        aggregation = Aggregation("example", AggregationType.AVG,
                                  [AggregationTrigger.INSERT, AggregationTrigger.DELETE, AggregationTrigger.UPDATE],
                                  "rate", None, None)
        document = {
            "id": 1,
            "name": "whatever",
            "rate": 4
        }
        AggregationService.execute_aggregation(aggregation, example_collection, None, document)
        self.assertTrue(mock_repository.called)
        expected_increment = AtomicIncrement("collection#example", "collection",
                                             [Counter("rate_count", Decimal(1), False),
                                              Counter("rate_sum", Decimal(4), False)])
        self.assertEqual(
            call(expected_increment),
            mock_increment_counter.call_args_list[0])

    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_collection_avg_update_decrease(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None
        example_collection = Collection("example", "id")
        aggregation = Aggregation("example", AggregationType.AVG,
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
        AggregationService.execute_aggregation(aggregation, example_collection, new_record, old_document)
        self.assertTrue(mock_repository.called)
        self.assertEqual(
            call(AtomicIncrement("collection#example", "collection",
                                 [
                                     Counter("rate_sum", Decimal(2), False)
                                 ]
                                 )),
            mock_increment_counter.call_args_list[0])

    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_collection_avg_update_increase(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None
        example_collection = Collection("example", "id")
        aggregation = Aggregation("example", AggregationType.AVG,
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
        AggregationService.execute_aggregation(aggregation, example_collection, new_record, old_document)
        self.assertTrue(mock_repository.called)
        self.assertEqual(
            call(AtomicIncrement("collection#example", "collection",
                                 [
                                     Counter("rate_sum", Decimal(4))
                                 ]
                                 )),
            mock_increment_counter.call_args_list[0])

    @patch.object(Repository, "increment_counter")
    @patch.object(Repository, "__init__")
    def test_not_matching_predicate(self, mock_repository, mock_increment_counter):
        mock_repository.return_value = None
        example_collection = Collection("example", "id")
        aggregation = Aggregation("example", AggregationType.AVG,
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
        AggregationService.execute_aggregation(aggregation, example_collection, new_record, old_document)
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
