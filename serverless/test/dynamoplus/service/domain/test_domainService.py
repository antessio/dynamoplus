import os
import unittest
from unittest.mock import patch

from mock import call

from dynamoplus.models.query.conditions import AnyMatch, Eq
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.system.index.index import Index
from dynamoplus.v2.repository.repositories import Repository, Model, QueryResult
from dynamoplus.v2.service.domain.domain_service import DomainService
from dynamoplus.v2.service.query_service import QueryService

domain_table_name = "domain"
system_table_name = "system"


class TestDomainService(unittest.TestCase):

    def setUp(self):
        os.environ["DYNAMODB_DOMAIN_TABLE"] = domain_table_name
        os.environ["DYNAMODB_SYSTEM_TABLE"] = system_table_name
        self.exampleCollectionMetadata = Collection("example", "id", "ordering")
        self.domainService = DomainService(self.exampleCollectionMetadata)

    @patch.object(Repository, "create")
    @patch.object(Repository, "__init__")
    def test_createDocument(self, mock_repository, mock_create):
        expected_id = 'randomId'
        document = {"id": expected_id, "fields": [{"field1": "string"}], "ordering": "1"}
        expected_model = Model("example#" + expected_id, "example", "1", document)
        mock_repository.return_value = None
        mock_create.return_value = expected_model
        result = self.domainService.create_document(document)
        self.assertIsNotNone(result)
        self.assertDictEqual(result, document)
        mock_repository.assert_called_once_with(domain_table_name)
        self.assertEqual(call(expected_model), mock_create.call_args_list[0])

    @patch.object(Repository, "update")
    @patch.object(Repository, "__init__")
    def test_updateDocument(self, mock_repository, mock_update):
        expected_id = 'randomId'
        document = {"id": expected_id, "fields": [{"field1": "string"}], "ordering": "1"}
        expected_model = Model("example#" + expected_id, "example", "1", document)
        mock_repository.return_value = None
        mock_update.return_value = expected_model
        result = self.domainService.update_document(document)
        self.assertIsNotNone(result)
        self.assertDictEqual(result, document)
        mock_repository.assert_called_once_with(domain_table_name)
        self.assertEqual(call(expected_model), mock_update.call_args_list[0])

    @patch.object(Repository, "delete")
    @patch.object(Repository, "__init__")
    def test_deleteCollection(self, mock_repository, mock_delete):
        expected_id = 'randomId'
        mock_repository.return_value = None
        self.domainService.delete_document(expected_id)
        mock_repository.assert_called_once_with(domain_table_name)
        self.assertTrue(mock_delete.called_with("example#" + expected_id, "example"))

    @patch.object(Repository, "get")
    @patch.object(Repository, "__init__")
    def test_getDocument(self, mock_repository, mock_get):
        expected_id = "randomId"
        document = {"id_key": expected_id, "fields": [{"field1": "string"}]}
        expected_model = Model("example#" + expected_id, "example", "1", document)
        mock_repository.return_value = None
        mock_get.return_value = expected_model
        found_document = self.domainService.get_document(expected_id)
        self.assertDictEqual(found_document, document)
        mock_repository.assert_called_once_with(domain_table_name)
        self.assertTrue(mock_get.called_with("example#" + expected_id, "example"))

    @patch.object(QueryService, "query")
    def test_find_all_documents(self, mock_query):
        # given
        mock_query.return_value = QueryResult([
            Model("example#1", "example", "1", {"id": "1", "attribute1": "1"}),
            Model("example#2", "example", "2", {"id": "2", "attribute1": "1"})
        ])
        # when
        documents, last_evaluated_key = self.domainService.find_all()
        # then
        self.assertEqual(2, len(documents))
        self.assertEqual(documents[0]["id"], "1")
        self.assertEqual(documents[1]["id"], "2")
        self.assertIsNone(last_evaluated_key)
        self.assertTrue(mock_query.called)
        mock_query.assert_called_once_with(self.exampleCollectionMetadata, AnyMatch(), None, None, None)

    @patch.object(QueryService, "query")
    def test_query(self, mock_query):
        # given
        mock_query.return_value = QueryResult([
            Model("example#1", "example", "1", {"id": "1", "attribute1": "1"}),
            Model("example#2", "example", "2", {"id": "2", "attribute1": "1"})
        ])
        # when
        index = Index("1", "example", ["attribute1"])
        predicate = Eq("attribute1", "1")
        documents, last_evaluated_key = self.domainService.query(predicate, index)
        # then
        self.assertEqual(2, len(documents))
        self.assertEqual(documents[0]["id"], "1")
        self.assertEqual(documents[1]["id"], "2")
        self.assertIsNone(last_evaluated_key)
        self.assertTrue(mock_query.called)
        mock_query.assert_called_once_with(self.exampleCollectionMetadata, predicate, index, None, None)

    # @patch.object(Repository,"create")
    # @patch.object(Repository, "__init__")
    # def test_createIndexWithOrdering(self,mock_repository,mock_create):
    #     expectedId = 'field1__field2.field21__ORDER_BY__field2.field21'
    #     expectedConditions=["field1","field2.field21"]
    #     targetIndex = {"name":expectedId, "collection":{"name": "example"},"conditions":expectedConditions,"orderingKey":"field2.field21"}
    #     indexMetadata = Collection("index","name")
    #     expectedModel = Model(indexMetadata, targetIndex)
    #     mock_repository.return_value=None
    #     mock_create.return_value=expectedModel
    #     index=Index("example",expectedConditions,"field2.field21")
    #     createdIndex = self.systemService.createIndex(index)
    #     indexName = createdIndex.indexName()
    #     self.assertEqual(indexName,expectedId)
    #     self.assertEqual(call(targetIndex),mock_create.call_args_list[0])

    # @patch.object(Repository,"create")
    # @patch.object(Repository, "__init__")
    # def test_createIndexWithNoOrdering(self,mock_repository,mock_create):
    #     expectedId = 'field1__field2.field21'
    #     expectedConditions=["field1","field2.field21"]
    #     targetIndex = {"name":expectedId, "collection":{"name": "example"},"conditions":expectedConditions,"orderingKey":None}
    #     indexMetadata = Collection("index","name")
    #     expectedModel = Model(indexMetadata, targetIndex)
    #     mock_repository.return_value=None
    #     mock_create.return_value=expectedModel
    #     index=Index("example",expectedConditions)
    #     createdIndex = self.systemService.createIndex(index)
    #     indexName = createdIndex.indexName()
    #     self.assertEqual(indexName,expectedId)
    #     self.assertEqual(call(targetIndex),mock_create.call_args_list[0])
