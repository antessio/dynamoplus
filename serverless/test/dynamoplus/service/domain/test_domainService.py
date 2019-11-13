import unittest
import decimal

from dynamoplus.models.indexes.indexes import Query
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.repository.models import Model, DataModel, QueryResult, IndexModel
from dynamoplus.service.domain.domain import DomainService
from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType
from dynamoplus.models.system.index.index import Index
from mock import call
from unittest.mock import patch


class TestDomainService(unittest.TestCase):

    def setUp(self):
        self.exampleCollectionMetadata = Collection("example", "id", "ordering")
        self.domainService = DomainService(self.exampleCollectionMetadata)

    @patch.object(DynamoPlusRepository, "create")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_createDocument(self, mock_repository, mock_create):
        expected_id = 'randomId'
        document = {"id_key": expected_id, "fields": [{"field1": "string"}]}
        expected_model = Model(self.exampleCollectionMetadata, document)
        mock_repository.return_value = None
        mock_create.return_value = expected_model
        result = self.domainService.create_document(document)
        self.assertIsNotNone(result)
        self.assertDictEqual(result, document)
        mock_repository.assert_called_once_with(self.exampleCollectionMetadata)
        self.assertEqual(call(document), mock_create.call_args_list[0])

    @patch.object(DynamoPlusRepository, "update")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_updateDocument(self, mock_repository, mock_update):
        expected_id = 'randomId'
        document = {"id_key": expected_id, "fields": [{"field1": "string"}]}
        expected_model = Model(self.exampleCollectionMetadata, document)
        mock_repository.return_value = None
        mock_update.return_value = expected_model
        result = self.domainService.update_document(document)
        self.assertIsNotNone(result)
        self.assertDictEqual(result, document)
        mock_repository.assert_called_once_with(self.exampleCollectionMetadata)
        self.assertEqual(call(document), mock_update.call_args_list[0])

    @patch.object(DynamoPlusRepository, "delete")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_deleteCollection(self, mock_repository, mock_delete):
        expected_id = 'randomId'
        mock_repository.return_value = None
        self.domainService.delete_document(expected_id)
        mock_repository.assert_called_once_with(self.exampleCollectionMetadata)
        self.assertTrue(mock_delete.called_with(expected_id))

    @patch.object(DynamoPlusRepository, "get")
    @patch.object(DynamoPlusRepository, "__init__")
    def test_getDocument(self, mock_repository, mock_get):
        expected_id = "randomId"
        document = {"id_key": expected_id, "fields": [{"field1": "string"}]}
        expected_model = Model(self.exampleCollectionMetadata, document)
        mock_repository.return_value = None
        mock_get.return_value = expected_model
        found_document = self.domainService.get_document(expected_id)
        self.assertDictEqual(found_document, document)
        mock_repository.assert_called_once_with(self.exampleCollectionMetadata)
        self.assertTrue(mock_get.called_with(expected_id))

    @patch.object(IndexDynamoPlusRepository, "find")
    @patch.object(IndexDynamoPlusRepository, "__init__")
    def test_find_all_documents(self, mock_index_repository, mock_find):
        # given
        mock_index_repository.return_value = None
        mock_find.return_value = QueryResult([
            Model(self.exampleCollectionMetadata, {"id": "1", "attribute1": "1"}).data_model,
            Model(self.exampleCollectionMetadata, {"id": "2", "attribute1": "1"}).data_model
        ])
        # when
        documents, last_evaluated_key = self.domainService.find_all()
        # then
        self.assertTrue(len(documents) == 2)
        self.assertEqual(documents[0]["id"], "1")
        self.assertEqual(documents[1]["id"], "2")
        self.assertIsNone(last_evaluated_key)

    @patch.object(IndexDynamoPlusRepository, "find")
    @patch.object(IndexDynamoPlusRepository, "__init__")
    def test_find_by_index(self, mock_index_repository, mock_find):
        # given
        index = Index("example", ["attribute1"])
        expected_example = {"attribute1": "1"}
        expected_query = Query(expected_example, index)
        mock_index_repository.return_value = None
        mock_find.return_value = QueryResult([
            IndexModel(self.exampleCollectionMetadata, {"id": "1", "attribute1": "1"}, index).data_model,
            IndexModel(self.exampleCollectionMetadata, {"id": "2", "attribute1": "1"}, index).data_model
        ])
        # when
        documents, last_evaluated_key = self.domainService.find_by_index(index, expected_example)
        # then
        self.assertEqual(len(documents), 2)
        self.assertEqual(documents[0]["id"], "1")
        self.assertEqual(documents[1]["id"], "2")
        self.assertIsNone(last_evaluated_key)
        self.assertEqual(call(expected_query), mock_find.call_args_list[0])

    # @patch.object(DynamoPlusRepository,"create")
    # @patch.object(DynamoPlusRepository, "__init__")
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

    # @patch.object(DynamoPlusRepository,"create")
    # @patch.object(DynamoPlusRepository, "__init__")
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
