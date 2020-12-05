import os
import unittest
from unittest.mock import patch

from mock import call

from dynamoplus.dynamo_plus_v2 import get, query
from dynamoplus.models.query.conditions import Eq
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.system.index.index import Index
from dynamoplus.v2.repository.repositories import QueryResult, Model
from dynamoplus.v2.service.query_service import QueryService
from dynamoplus.v2.service.system.system_service import CollectionService, IndexService


class TestDynamoPlusHandler(unittest.TestCase):

    def setUp(self):
        os.environ['ENTITIES'] = 'index,collection,client_authorization'
        os.environ["DYNAMODB_DOMAIN_TABLE"] = "example-domain"
        os.environ["DYNAMODB_SYSTEM_TABLE"] = "example-system"

    def tearDown(self):
        del os.environ['ENTITIES']
        del os.environ["DYNAMODB_DOMAIN_TABLE"]
        del os.environ["DYNAMODB_SYSTEM_TABLE"]



    @patch.object(CollectionService, "get_collection")
    def test_getSystemCollection_metadata(self,  mock_get_system_collection):
        mock_get_system_collection.return_value = Collection("example", "id", "ordering")
        collection_metadata = get("collection", "example")
        self.assertDictEqual(collection_metadata,
                             dict(id_key="id", name="example", ordering_key="ordering", attribute_definition=None, auto_generate_id=False))
        self.assertTrue(mock_get_system_collection.called_with("example"))


    @patch.object(QueryService, "query")
    @patch.object(IndexService, "get_index_matching_fields")
    @patch.object(CollectionService, "get_collection")
    def test_get_documents_by_index(self, mock_get_collection,mock_get_index_matching_fields,mock_query):
        expected_collection = Collection("example", "id", "ordering")
        mock_get_collection.return_value = expected_collection
        expected_index = Index("example", ["attribute1"])
        expected_predicate = Eq("attribute1","1")
        mock_get_index_matching_fields.return_value=expected_index
        expected_documents = [
            Model(None,None,None,{"id": "1", "attribute1": "1"}),
            Model(None,None,None,{"id": "2", "attribute1": "1"})
        ]
        mock_query.return_value = QueryResult(expected_documents, None)
        documents = query("example", {"matches": {"eq":{"field_name":"attribute1", "value":"1"}}})
        self.assertEqual(len(documents), len(expected_documents))
        self.assertTrue(mock_get_collection.called_with("example"))
        self.assertTrue(mock_get_index_matching_fields.called_with("attribute1"))
        self.assertEqual(call(expected_collection,expected_predicate,expected_index, None, None), mock_query.call_args_list[0])
