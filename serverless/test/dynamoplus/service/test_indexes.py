import unittest
import decimal
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.repository.repositories import Repository
from dynamoplus.models.indexes.indexes import Query, Index
from dynamoplus.repository.models import QueryResult, Model
from dynamoplus.service.indexes import IndexService

from unittest.mock import patch

class TestIndexService(unittest.TestCase):

    def setUp(self):
        pass


    @patch.object(Repository,"find")
    @patch.object(Repository, "__init__")
    def test_findDocument(self, mock_repository,mock_find):
        documentTypeConfiguration = DocumentTypeConfiguration("example","id","ordering")
        index = Index("example",["attribute1"])
        data =[
            Model(documentTypeConfiguration,{"pk": "example#1","sk":"example#attribute1", "data":"value1", "document":{"attribute1":"value1"} }),
            Model(documentTypeConfiguration,{"pk": "example#2","sk":"example#attribute1", "data":"value1", "document":{"attribute1":"value1"} }),
            Model(documentTypeConfiguration,{"pk": "example#3","sk":"example#attribute1", "data":"value1", "document":{"attribute1":"value1"} })
            ]
        mock_repository.return_value=None
        mock_find.return_value=QueryResult(data)
        self.indexService = IndexService(documentTypeConfiguration,index)
        result,lastKey = self.indexService.findDocuments({"attribute1":"value1"})
        self.assertEqual(len(result),3)
        self.assertIsNone(lastKey)