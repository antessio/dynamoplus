import unittest
import os
import sys
from unittest.mock import patch
from dynamoplus.http.handler import HttpHandler
from dynamoplus.repository.Repository import Repository
from dynamoplus.service.IndexService import IndexUtils

class TestHttpHandler(unittest.TestCase):

    @patch.object(Repository, "getEntityDTO")
    @patch.object(Repository, "get")
    def test_getFound(self, mock_get,mock_getEntityDTO):
        expectedRow = {"id": "1", "sk": "sk","pk":"pk", "data":"data"}
        expectedResult = {"id":"randomUid","attr1":"value1"}
        mock_get.return_value=expectedRow
        mock_getEntityDTO.return_value=expectedResult
        handler = HttpHandler("host#id#creation_date_time,category#id#order","host")
        result = handler.get({"entity":"host", "id":"randomUid"})
        self.assertEqual(result["statusCode"],200)
        self.assertEqual(result["body"], handler._formatJson(expectedResult))
        mock_get.assert_called_with("randomUid")
        mock_getEntityDTO.assert_called_with(expectedRow)
    
    @patch.object(Repository, "getEntityDTO")
    @patch.object(Repository, "get")
    def test_getNotFound(self, mock_get,mock_getEntityDTO):
        expectedRow = {"id": "1", "sk": "sk","pk":"pk", "data":"data"}
        expectedResult = {"id":"randomUid","attr1":"value1"}
        mock_get.return_value=None
        handler = HttpHandler("host#id#creation_date_time,category#id#order","host")
        result = handler.get({"entity":"host", "id":"randomUid"})
        self.assertEqual(result["statusCode"],404)
        mock_get.assert_called_with("randomUid")
        self.assertFalse(mock_getEntityDTO.called)

    @patch.object(Repository, "getEntityDTO")
    @patch.object(Repository, "get")
    def test_getWrongEntity(self, mock_get,mock_getEntityDTO):
        expectedRow = {"id": "1", "sk": "sk","pk":"pk", "data":"data"}
        expectedResult = {"id":"randomUid","attr1":"value1"}
        mock_get.return_value=None
        handler = HttpHandler("host#id#creation_date_time,category#id#order","host")
        result = handler.get({"entity":"nonExisting", "id":"randomUid"})
        self.assertEqual(result["statusCode"],400)
        self.assertFalse(mock_get.called)
        self.assertFalse(mock_getEntityDTO.called)

    @patch.object(Repository, "getEntityDTO")
    @patch.object(Repository, "create")
    def test_create(self, mock_create,mock_getEntityDTO):
        expectedRow = {"id": "randomUid", "sk": "sk","pk":"pk", "data":"data"}
        expectedResult = {"id":"randomUid","attr1":"value1"}
        mock_create.return_value=expectedRow
        mock_getEntityDTO.return_value=expectedResult
        handler = HttpHandler("host#id#creation_date_time,category#id#order","host")
        result = handler.create({"entity":"host"},body="{\"attr1\": \"value1\"}")
        self.assertEqual(result["statusCode"],201)
        self.assertEqual(result["body"], handler._formatJson(expectedResult))
        self.assertTrue(mock_create.called)
        mock_getEntityDTO.assert_called_with(expectedRow)
    
    @patch.object(Repository, "getEntityDTO")
    @patch.object(Repository, "update")
    def test_update(self, mock_update,mock_getEntityDTO):
        expectedRow = {"id": "randomUid", "sk": "sk","pk":"pk", "data":"data"}
        expectedResult = {"id":"randomUid","attr1":"value1"}
        mock_update.return_value=expectedRow
        mock_getEntityDTO.return_value=expectedResult
        handler = HttpHandler("host#id#creation_date_time,category#id#order","host")
        result = handler.update({"entity":"host"},body="{\"attr1\": \"value1\"}")
        self.assertEqual(result["statusCode"],200)
        self.assertEqual(result["body"], handler._formatJson(expectedResult))
        self.assertTrue(mock_update.called)
        mock_getEntityDTO.assert_called_with(expectedRow)
    
    @patch.object(Repository, "delete")
    def test_delete(self, mock_delete):
        handler = HttpHandler("host#id#creation_date_time,category#id#order","host")
        result = handler.delete({"entity":"host","id":"randomUid"},body="{\"attr1\": \"value1\"}")
        self.assertEqual(result["statusCode"],200)
        mock_delete.assert_called_with("randomUid")

    @patch.object(IndexUtils, "buildIndex")
    @patch.object(Repository, "find")
    def test_query(self, mock_find, mock_buildIndex):
        foundIndex = {
            "tablePrefix": "host",
            "conditions": ["attr1","attr2"]
        }
        orderBy="top"
        expectedResult = [{"id": "1","attr1":"value1","attr2":"value2"}]
        mock_buildIndex.return_value=orderBy,foundIndex
        mock_find.return_value=expectedResult
        handler = HttpHandler("host#id#creation_date_time,category#id#order","host")
        result = handler.query({"entity": "host", "queryId": "attr1__attr2"},queryStringParameters={},body="{\"attr1\": \"value1\",\"attr2\":\"value2\"}",headers=[])
        self.assertEqual(result["statusCode"],200)
        #self.assertEqual(result["body"], handler._formatJson(expectedResult))
        #mock_find.assert_called_with()

if __name__ == '__main__':
    unittest.main()
