import os
import unittest

from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.system.index.index import Index, IndexConfiguration
from dynamoplus.v2.indexing_service_v2 import create_indexes,update_indexes,delete_indexes

from mock import call
from unittest.mock import patch

from aws.dynamodb.dynamodbdao import DynamoDBDAO, DynamoDBModel
from dynamoplus.v2.service.system.system_service import CollectionService, IndexService
from dynamoplus.v2.service.system.system_service_v2 import AggregationConfigurationService

domain_table_name = "domain"
system_table_name = "system"


class TestIndexService(unittest.TestCase):

    def setUp(self):
        os.environ["DYNAMODB_DOMAIN_TABLE"] = domain_table_name
        os.environ["DYNAMODB_SYSTEM_TABLE"] = system_table_name
        os.environ["STAGE"] = 'local'
        os.environ.setdefault("STAGE", "local")

    @patch.object(AggregationConfigurationService, "get_aggregation_configurations_by_collection_name_generator")
    @patch.object(DynamoDBDAO, "create")
    @patch.object(DynamoDBDAO, "__init__")
    @patch.object(IndexService,"get_indexes_from_collection_name_generator")
    @patch.object(CollectionService, "get_collection")
    def test_create_indexes(self,mock_get_collection,mock_get_indexes_from_collection_name_generator,
                            mock_repository,mock_repository_create, mock_aggregations):
        collection_name = "example"
        example_record = {
            "id": "1",
            "attribute_1": "value_1",
            "attribute_2": "value_2",
            "attribute_3":{
                "attribute_31": "value_31"
            }
        }
        mock_get_indexes_from_collection_name_generator.return_value = []
        mock_repository.return_value = None
        mock_get_collection.return_value = Collection("example","id")
        mock_get_indexes_from_collection_name_generator.return_value = [
            Index("example", ["attribute_1"]),
            Index("example", ["attribute_2", "attribute_1"]),
            Index("example", ["attribute_3.attribute_31"], IndexConfiguration.OPTIMIZE_READ, "attribute_1")
        ]
        create_indexes(collection_name,example_record)
        mock_get_collection.assert_called_once_with(collection_name)
        mock_get_indexes_from_collection_name_generator.assert_called_once_with(collection_name)
        mock_repository_create.assert_has_calls([call(DynamoDBModel("example#1", "example#attribute_1", "value_1", example_record)),
                                                 call(DynamoDBModel("example#1", "example#attribute_2#attribute_1", "value_2#value_1", example_record)),
                                                 call(DynamoDBModel("example#1", "example#attribute_3.attribute_31", "value_31#value_1", example_record, ))])

    @patch.object(AggregationConfigurationService, "get_aggregation_configurations_by_collection_name_generator")
    @patch.object(DynamoDBDAO, "update")
    @patch.object(DynamoDBDAO, "__init__")
    @patch.object(IndexService, "get_indexes_from_collection_name_generator")
    @patch.object(CollectionService, "get_collection")
    def test_update_indexes(self, mock_get_collection, mock_get_indexes_from_collection_name_generator,
                            mock_repository, mock_repository_update,mock_aggregations):
        collection_name = "example"
        example_record = {
            "id": "1",
            "attribute_1": "value_1",
            "attribute_2": "value_2",
            "attribute_3": {
                "attribute_31": "value_31"
            }
        }
        old_record = {
            "id": "1",
            "attribute_1": "value_1e",
            "attribute_2": "value_2e",
            "attribute_3": {
                "attribute_31": "value_31e"
            }
        }
        mock_repository.return_value = None
        mock_aggregations.return_value = []
        mock_get_collection.return_value = Collection("example", "id")
        mock_get_indexes_from_collection_name_generator.return_value = [
            Index("example", ["attribute_1"]),
            Index("example", ["attribute_2", "attribute_1"]),
            Index("example", ["attribute_3.attribute_31"], IndexConfiguration.OPTIMIZE_READ, "attribute_1")
        ]
        update_indexes(collection_name, old_record, example_record)
        mock_get_collection.assert_called_once_with(collection_name)
        mock_get_indexes_from_collection_name_generator.assert_called_once_with(collection_name)
        mock_repository_update.assert_has_calls(
            [call(DynamoDBModel("example#1", "example#attribute_1", "value_1", example_record)),
             call(DynamoDBModel("example#1", "example#attribute_2#attribute_1", "value_2#value_1", example_record)),
             call(DynamoDBModel("example#1", "example#attribute_3.attribute_31", "value_31#value_1", example_record, ))])

    @patch.object(AggregationConfigurationService, "get_aggregation_configurations_by_collection_name_generator")
    @patch.object(DynamoDBDAO, "delete")
    @patch.object(DynamoDBDAO, "__init__")
    @patch.object(IndexService, "get_indexes_from_collection_name_generator")
    @patch.object(CollectionService, "get_collection")
    def test_delete_indexes(self, mock_get_collection, mock_get_indexes_from_collection_name_generator,
                            mock_repository, mock_repository_delete, mock_aggregations):
        collection_name = "example"
        example_record = {
            "id": "1",
            "attribute_1": "value_1",
            "attribute_2": "value_2",
            "attribute_3": {
                "attribute_31": "value_31"
            }
        }
        mock_aggregations.return_value = []
        mock_repository.return_value = None
        mock_get_collection.return_value = Collection("example", "id")
        mock_get_indexes_from_collection_name_generator.return_value = [
            Index("example", ["attribute_1"]),
            Index("example", ["attribute_2", "attribute_1"]),
            Index("example", ["attribute_3.attribute_31"], "attribute_1")
        ]
        delete_indexes(collection_name, example_record)
        mock_get_collection.assert_called_once_with(collection_name)
        mock_get_indexes_from_collection_name_generator.assert_called_once_with(collection_name)
        mock_repository_delete.assert_has_calls(
            [call("example#1", "example#attribute_1"),
             call("example#1", "example#attribute_2#attribute_1"),
             call("example#1", "example#attribute_3.attribute_31")])



