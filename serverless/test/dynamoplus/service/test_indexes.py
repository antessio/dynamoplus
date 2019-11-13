import unittest
import decimal
from dynamoplus.models.indexes.indexes import Index
#from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.models.indexes.indexes import Query, Index
from dynamoplus.repository.models import QueryResult, Model
from dynamoplus.service.indexes import IndexService

from unittest.mock import patch

class TestIndexService(unittest.TestCase):

    def setUp(self):
        pass


    @patch.object(IndexDynamoPlusRepository,"find")
    @patch.object(IndexDynamoPlusRepository, "__init__")
    def test_findDocument(self, mock_repository,mock_find):
        #documentTypeConfiguration = DocumentTypeConfiguration("example","id","ordering")
        collectionConfiguration = Collection("example","id","ordering")
        index = Index("example",["attribute1"])
        data =[
            Model(collectionConfiguration,{"pk": "example#1","sk":"example#attribute1", "data":"value1"}),
            Model(collectionConfiguration,{"pk": "example#2","sk":"example#attribute1", "data":"value1"}),
            Model(collectionConfiguration,{"pk": "example#3","sk":"example#attribute1", "data":"value1"})
            ]
        mock_repository.return_value=None
        mock_find.return_value=QueryResult(data)
        self.indexService = IndexService(collectionConfiguration,index)
        result,lastKey = self.indexService.find_documents({"attribute1": "value1"})
        self.assertEqual(len(result),3)
        self.assertIsNone(lastKey)