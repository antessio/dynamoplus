import unittest
import os

from dynamoplus.service.domain.domain import DomainService
from dynamoplus.service.system.system import SystemService
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.system.index.index import Index
from dynamoplus.dynamo_plus import get,create,update,delete,query
from mock import call
from unittest.mock import patch


class TestDynamoPlusHandler(unittest.TestCase):

    def setUp(self):
        os.environ['ENTITIES'] = 'index,collection,client_authorization'

    def tearDown(self):
        del os.environ['ENTITIES']



    @patch.object(SystemService, "get_collection_by_name")
    @patch.object(SystemService, "__init__")
    def test_getSystemCollectionmetadata(self, mock_system_service, mock_get_system_collection):
        mock_system_service.return_value = None
        mock_get_system_collection.return_value = Collection("example", "id", "ordering")
        collection_metadata = get("collection", "example")
        self.assertDictEqual(collection_metadata,
                             dict(id_key="id", name="example", ordering_key="ordering", attribute_definition=None))
        self.assertTrue(mock_get_system_collection.called_with("example"))

    @patch.object(DomainService, "find_all")
    @patch.object(DomainService, "__init__")
    @patch.object(SystemService, "get_collection_by_name")
    @patch.object(SystemService, "__init__")
    def test_get_all_documents(self, mock_system_service, mock_get_collection_by_name, mock_domain_service,
                               mock_find_all):
        mock_system_service.return_value = None
        mock_get_collection_by_name.return_value = Collection("example", "id", "ordering")
        mock_domain_service.return_value = None
        expected_documents = [
            {"id": "1", "attribute1": "1"},
            {"id": "2", "attribute1": "1"},
            {"id": "3", "attribute1": "2"}
        ]
        mock_find_all.return_value = expected_documents, None
        documents,last_key = query("example")
        self.assertIsNone(last_key)
        self.assertEqual(len(documents), len(expected_documents))
        self.assertTrue(mock_get_collection_by_name.called_with("example"))
        self.assertTrue(mock_find_all.called)

    @patch.object(DomainService, "find_by_index")
    @patch.object(DomainService, "__init__")
    @patch.object(SystemService, "get_index")
    @patch.object(SystemService, "get_collection_by_name")
    @patch.object(SystemService, "__init__")
    def test_get_documents_by_index(self, mock_system_service, mock_get_collection_by_name, mock_get_index,
                                    mock_domain_service, mock_find_by_index):
        mock_system_service.return_value = None
        mock_get_collection_by_name.return_value = Collection("example", "id", "ordering")
        mock_domain_service.return_value = None
        expected_index = Index("1","example", ["attribute1"])
        mock_get_index.return_value=expected_index
        expected_document_example = {"attribute1": "1"}
        expected_documents = [
            {"id": "1", "attribute1": "1"},
            {"id": "2", "attribute1": "1"}
        ]
        mock_find_by_index.return_value = expected_documents, None
        documents = query("example", "attribute1", expected_document_example)
        self.assertEqual(len(documents), len(expected_documents))
        self.assertTrue(mock_get_collection_by_name.called_with("example"))
        self.assertTrue(mock_get_index.called_with("attribute1"))
        self.assertEqual(call(expected_index,expected_document_example,None,None),mock_find_by_index.call_args_list[0])
