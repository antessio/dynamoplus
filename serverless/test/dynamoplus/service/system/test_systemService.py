import unittest
import decimal
from dynamoplus.repository.repositories import DomainRepository
from dynamoplus.repository.models import Model
from dynamoplus.service.system.system import SystemService
from dynamoplus.models.system.collection.collection import Collection,AttributeDefinition, AttributeType
from dynamoplus.models.system.index.index import Index
from mock import call
from unittest.mock import patch

class TestIndexService(unittest.TestCase):

    def setUp(self):
        self.systemService = SystemService()

    @patch.object(DomainRepository,"create")
    @patch.object(DomainRepository, "__init__")
    def test_createCollection(self, mock_repository,mock_create):
        expectedId = 'example'
        targetCollection = {"name":"example","idKey":"id","ordering":None}
        document={"name": expectedId, "idKey":"id"}
        collectionMetadata = Collection("collection","name")
        expectedModel = Model(collectionMetadata, document)
        mock_repository.return_value=None
        mock_create.return_value=expectedModel

        targetMetadata = Collection("example","id")
        createdCollection = self.systemService.createCollection(targetMetadata)
        collectionId = createdCollection.name
        self.assertEqual(collectionId,expectedId)
        self.assertEqual(call(targetCollection),mock_create.call_args_list[0])

    
    # @patch.object(DomainRepository,"update")
    # @patch.object(DomainRepository, "__init__")
    # def test_updateCollection(self,mock_repository,mock_update):
    #     expectedId = 'example'
    #     mock_repository.return_value=None
    #     collectionMetadata = Collection("collection","name")
    #     document={"name": expectedId,"idKey":"id"}
    #     expectedModel = Model(collectionMetadata, document)
    #     mock_update.return_value=expectedModel
    #     targetCollection = {"name":expectedId,"fields":[{"field1":"string"}]}
    #     targetMetadata=Collection("example","id",None,[AttributeDefinition("field1",AttributeType.STRING)])
    #     self.systemService.updateCollection(targetMetadata)
    #     self.assertEqual(call(targetCollection),mock_update.call_args_list[0])

    @patch.object(DomainRepository,"delete")
    @patch.object(DomainRepository, "__init__")
    def test_deleteCollection(self,mock_repository,mock_delete):
        expectedId = 'example'
        mock_repository.return_value=None
        self.systemService.deleteCollection(expectedId)
        self.assertTrue(mock_delete.called_with(expectedId))

    @patch.object(DomainRepository,"get")
    @patch.object(DomainRepository, "__init__")
    def test_getCollection(self,mock_repository,mock_get):
        expectedId = 'example'
        mock_repository.return_value=None
        collectionMetadata = Collection("collection","name")
        document={"name": expectedId,"fields":[{"field1":"string"}]}
        expectedModel = Model(collectionMetadata, document)
        mock_get.return_value=expectedModel
        result=self.systemService.getCollectionByName(expectedId)
        self.assertIn("fields",result)
        self.assertTrue(mock_get.called_with(expectedId))
    
    @patch.object(DomainRepository,"create")
    @patch.object(DomainRepository, "__init__")
    def test_createIndexWithOrdering(self,mock_repository,mock_create):
        expectedId = 'field1__field2.field21__ORDER_BY__field2.field21'
        expectedConditions=["field1","field2.field21"]
        targetIndex = {"name":expectedId, "collection":{"name": "example"},"conditions":expectedConditions,"orderingKey":"field2.field21"}
        indexMetadata = Collection("index","name")
        expectedModel = Model(indexMetadata, targetIndex)
        mock_repository.return_value=None
        mock_create.return_value=expectedModel
        index=Index("example",expectedConditions,"field2.field21")
        createdIndex = self.systemService.createIndex(index)
        indexName = createdIndex.indexName()
        self.assertEqual(indexName,expectedId)
        self.assertEqual(call(targetIndex),mock_create.call_args_list[0])
    
    @patch.object(DomainRepository,"create")
    @patch.object(DomainRepository, "__init__")
    def test_createIndexWithNoOrdering(self,mock_repository,mock_create):
        expectedId = 'field1__field2.field21'
        expectedConditions=["field1","field2.field21"]
        targetIndex = {"name":expectedId, "collection":{"name": "example"},"conditions":expectedConditions,"orderingKey":None}
        indexMetadata = Collection("index","name")
        expectedModel = Model(indexMetadata, targetIndex)
        mock_repository.return_value=None
        mock_create.return_value=expectedModel
        index=Index("example",expectedConditions)
        createdIndex = self.systemService.createIndex(index)
        indexName = createdIndex.indexName()
        self.assertEqual(indexName,expectedId)
        self.assertEqual(call(targetIndex),mock_create.call_args_list[0])