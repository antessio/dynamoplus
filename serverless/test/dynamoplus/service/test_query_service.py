import os
import unittest

from dynamoplus.models.query.conditions import Eq, Range, AnyMatch
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.system.index.index import Index, IndexConfiguration
from dynamoplus.v2.repository.repositories import QueryRepository, QueryResult, Model, Repository
from dynamoplus.v2.service.query_service import QueryService

from mock import call
from unittest.mock import patch

domain_table_name = "domain"
system_table_name = "system"


class QueryServiceTest(unittest.TestCase):

    def setUp(self):
        os.environ["DYNAMODB_DOMAIN_TABLE"] = domain_table_name
        os.environ["DYNAMODB_SYSTEM_TABLE"] = system_table_name


    @patch.object(QueryRepository, "query_all")
    @patch.object(QueryRepository, "__init__")
    def test_query_all_generator(self, mock_query_repository, mock_query_all):
        mock_query_repository.return_value = None
        page1 = QueryResult([
            Model("example#1", "example", "1",None),
            Model("example#2", "example", "2",None),
            Model("example#3", "example", "3",None),
            Model("example#4", "example", "4",None),
            Model("example#5", "example", "5",None)]
            ,Model("example#5", "example", "5",None))
        page2 = QueryResult([
            Model("example#6", "example", "6",None),
            Model("example#7", "example", "7",None),
            Model("example#8", "example", "8",None),
            Model("example#9", "example", "9",None),
            Model("example#10", "example", "10",None)]
            , Model("example#10", "example", "10",None))
        page3 = QueryResult([
            Model("example#11", "example", "11",None),
            Model("example#12", "example", "12",None),
            Model("example#13", "example", "13",None),
            Model("example#14", "example", "14",None),
            Model("example#15", "example", "15",None)],
        None)


        expected_result = page1.data + page2.data + page3.data
        mock_query_all.side_effect = [
            page1,
            page2,
            page3
        ]
        expected_starting_after_1 = Model("example#5", "example", "5", None)
        expected_starting_after_2 = Model("example#10", "example", "10", None)


        collection = Collection("example", "id")
        index = Index("example", ["name"], IndexConfiguration.OPTIMIZE_READ)
        limit = 20
        query_result = list(QueryService.query_generator(collection, AnyMatch(), index))
        self.assertEqual(expected_result, query_result)
        mock_query_all.assert_has_calls([
            call("example", None, limit),
            call("example", expected_starting_after_1, limit),
            call("example", expected_starting_after_2, limit)
        ])


    @patch.object(QueryRepository, "query_all")
    @patch.object(QueryRepository, "__init__")
    def test_query_all_generator_with_filter(self, mock_query_repository, mock_query_all):
        mock_query_repository.return_value = None
        page1 = QueryResult([
            Model("example#1", "example", "1",None),
            Model("example#2", "example", "2",None),
            Model("example#3", "example", "3",None),
            Model("example#4", "example", "4",None),
            Model("example#5", "example", "5",None)]
            ,Model("example#5", "example", "5",None))
        page2 = QueryResult([
            Model("example#6", "example", "6",None),
            Model("example#7", "example", "7",None),
            Model("example#8", "example", "8",None),
            Model("example#9", "example", "9",None),
            Model("example#10", "example", "10",None)]
            , Model("example#10", "example", "10",None))
        page3 = QueryResult([
            Model("example#11", "example", "11",None),
            Model("example#12", "example", "12",None),
            Model("example#13", "example", "13",None),
            Model("example#14", "example", "14",None),
            Model("example#15", "example", "15",None)],
        None)


        expected_result = [Model("example#3", "example", "3",None)]
        mock_query_all.side_effect = [page1]

        collection = Collection("example", "id")
        index = Index("example", ["name"], IndexConfiguration.OPTIMIZE_READ)
        limit = 20
        generator = QueryService.query_generator(collection, AnyMatch(), index)

        filtered = filter(lambda r: r.data == '3', generator)
        query_result = list(filtered)
        self.assertEqual(expected_result, query_result)
        mock_query_all.assert_has_calls([
            call("example", None, limit)
        ])

    @patch.object(Repository, "get")
    @patch.object(Repository, "__init__")
    @patch.object(QueryRepository, "query_all")
    @patch.object(QueryRepository, "__init__")
    def test_query_all(self, mock_query_repository, mock_query_all,
                       mock_repository, mock_repository_get):
        mock_query_repository.return_value = None
        mock_repository.return_value = None
        partial_result = QueryResult([Model("example#1", "example", "1", None)])
        expected_model = Model("example#1", "example", "1", {"id": "1", "name": "my_name", "field_1": "value_1"})
        expected_model_starts_from = Model("example#0", "example", "0", None)
        expected_result = QueryResult([expected_model])
        mock_query_all.return_value = partial_result
        mock_repository_get.side_effect = [expected_model_starts_from, expected_model]

        collection = Collection("example", "id")
        index = Index("example", ["name"], IndexConfiguration.OPTIMIZE_WRITE)
        start_from = "0"
        limit = 10
        query_result = QueryService.query(collection, AnyMatch(), index, start_from, limit)
        self.assertEqual(expected_result, query_result)
        mock_query_all.assert_called_once_with("example", expected_model_starts_from,limit)
        mock_repository_get.assert_has_calls(
            [call("example#0", "example"),
             call("example#1", "example")]
        )


    @patch.object(QueryRepository, "query_all")
    @patch.object(QueryRepository, "__init__")
    def test_query_all_with_starting_after_as_model(self, mock_query_repository, mock_query_all):
        mock_query_repository.return_value = None
        partial_result = QueryResult([Model("example#1", "example", "1", None)])
        expected_model = Model("example#1", "example", "1", {"id": "1", "name": "my_name", "field_1": "value_1"})

        expected_result = QueryResult([expected_model])
        mock_query_all.return_value = partial_result


        collection = Collection("example", "id")
        index = Index("example", ["name"], IndexConfiguration.OPTIMIZE_WRITE)
        start_from = Model("example#0","example","0",{})
        limit = 10

        query_result = QueryService.__query(collection, AnyMatch(), index, start_from, limit)
        self.assertEqual(expected_result, query_result)
        mock_query_all.assert_called_once_with("example", start_from, limit)


    @patch.object(Repository, "get")
    @patch.object(Repository, "__init__")
    @patch.object(QueryRepository, "query_range")
    @patch.object(QueryRepository, "__init__")
    def test_query_range(self, mock_query_repository, mock_query_range,
                               mock_repository, mock_repository_get):
        mock_query_repository.return_value = None
        mock_repository.return_value = None
        partial_result = QueryResult([Model("example#1", "example", "1", None)])
        expected_model = Model("example#1", "example", "1", {"id": "1", "name": "my_name", "field_1": "value_1"})
        expected_model_starts_from = Model("example#0", "example", "0", None)
        expected_result = QueryResult([expected_model])
        mock_query_range.return_value = partial_result
        mock_repository_get.side_effect = [expected_model_starts_from, expected_model]

        collection = Collection("example", "id")
        predicate = Range("name","001","003")
        index = Index("example", ["name"], IndexConfiguration.OPTIMIZE_WRITE)
        start_from = "0"
        limit = 10
        query_result = QueryService.query(collection, predicate, index, start_from, limit)
        self.assertEqual(expected_result, query_result)
        mock_query_range.assert_called_once_with("example#name", "001","003", limit,expected_model_starts_from)
        mock_repository_get.assert_has_calls(
            [call("example#0", "example#name"),
             call("example#1", "example")]
        )

    @patch.object(Repository, "get")
    @patch.object(Repository, "__init__")
    @patch.object(QueryRepository, "query_begins_with")
    @patch.object(QueryRepository, "__init__")
    def test_query_begins_with(self, mock_query_repository, mock_query_begins_with,
                               mock_repository, mock_repository_get):
        mock_query_repository.return_value = None
        mock_repository.return_value = None
        partial_result = QueryResult([Model("example#1", "example", "1", None)])
        expected_model = Model("example#1", "example", "1", {"id": "1", "name": "my_name", "field_1": "value_1"})
        expected_model_starts_from = Model("example#0", "example", "0", None)
        expected_result = QueryResult([expected_model])
        mock_query_begins_with.return_value = partial_result
        mock_repository_get.side_effect = [expected_model_starts_from, expected_model]

        collection = Collection("example", "id")
        predicate = Eq("name", "my_name")
        index = Index("example", ["name"], IndexConfiguration.OPTIMIZE_WRITE)
        start_from = "0"
        limit = 10
        query_result = QueryService.query(collection, predicate, index, start_from, limit)
        self.assertEqual(expected_result, query_result)
        mock_query_begins_with.assert_called_once_with("example#name", "my_name", expected_model_starts_from, limit)
        mock_repository_get.assert_has_calls(
            [call("example#0", "example#name"),
             call("example#1", "example")]
        )

