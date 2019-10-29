from typing import * 

from dynamoplus.repository.repositories import DomainRepository
from dynamoplus.repository.models import Model
from dynamoplus.models.system.collection.collection import Collection,AttributeDefinition, AttributeType
from dynamoplus.models.system.index.index import Index

collectionMetadata = Collection("collection","name")
indexMetadata = Collection("index","name")
class SystemService:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    

    def createCollection(self,metadata:Collection):
        collection=self._fromCollectionToDict(metadata)
        repository = DomainRepository(collectionMetadata)
        model=repository.create(collection)
        return self._fromDictToCollection(model.fromDynamoDbItem())
    # def updateCollection(self, metadata:Collection):
    #     collection=self.fromCollectionToDict(metadata)
    #     repository = DomainRepository(collectionMetadata)
    #     model = repository.update(collection)
    #     return self.fromDictToCollection(model.document)
    def deleteCollection(self,name:str):
        DomainRepository(collectionMetadata).delete(name)
    def getCollectionByName(self,name:str):
        model=DomainRepository(collectionMetadata).get(name)
        return model.document

    def createIndex(self,i:Index):
        index = self._fromIndexToDict(i)
        repository = DomainRepository(indexMetadata)
        model=repository.create(index)
        return self._fromDictToIndex(model.fromDynamoDbItem())

    def getIndex(self, name:str):
        model = DomainRepository(indexMetadata).get(name)
        return self._fromDictToIndex(model.fromDynamoDbItem())
    def getIndexFromCollectioName(self, collectionName:str):
        pass
    
    def _fromDictToIndex(self, d:dict):
        return Index(d["collection"]["name"],d["conditions"],d["orderingKey"] if "orderingKey" in d else None)
    def _fromIndexToDict(self,indexMetadata:Index):
        return {
            "name": indexMetadata.indexName(),
            "collection":{
                "name": indexMetadata.collectionName
            },
            "orderingKey": indexMetadata.orderingKey ,
            "conditions": indexMetadata.conditions
            
        }
    def _fromDictToCollection(self, d:dict):
        return Collection(d["name"],d["idKey"],d["ordering"] if "ordering" in d else None)
    def _fromCollectionToDict(self,collection:Collection):
        d={
            "name": collection.name,
            "idKey": collection.idKey,
            "ordering": collection.orderingKey
            ## TODO attributes definition
        }
        # if collection.attributeDefinition:

        return d
    '''   
    - get indexes for a collection (used in indexing)
    - get index by name (used in query)
    - delete index
    '''