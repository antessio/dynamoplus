from typing import *
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.service.indexes import IndexService
from dynamoplus.repository.repositories import DomainRepository
from dynamoplus.models.indexes.indexes import Query, Index
from dynamoplus.repository.models import QueryResult, Model

collectionMetadata = Collection("collection","name")
indexMetadata = Collection("index","name")
systemCollections={
    "collection": collectionMetadata,
    "index": indexMetadata
}
class DynamoPlusService(object):
    def __init__(self):
        pass
        
    def getSystemCollectionConfigurationFromCollectionName(self, systemCollectionName:str):
        if systemCollectionName in systemCollections:
            return systemCollections[systemCollectionName]
    
    def getCustomCollectionConfigurationFromCollectionName(self, documentType:str):
        collection = self.getSystemCollectionConfigurationFromCollectionName("collection")
        index = Index(documentType,["name"])
        query = Query({"name": documentType},index)
        data, lastKey=IndexService(collection,index).findDocuments({"name": documentType})
        if len(data)>0:
            documenTypeDict = data[0]
            return Collection(documentType,documenTypeDict["idKey"],documenTypeDict["orderingKey"])
    
    def getCollectionConfigurationFromCollectionName(self, collectionName:str):
        documentTypeConfiguration = self.getSystemCollectionConfigurationFromCollectionName(collectionName)
        if documentTypeConfiguration:
            return documentTypeConfiguration
        else:
            return self.getCustomCollectionConfigurationFromCollectionName(collectionName)


    def getIndexesFromCollecionName(self, collectionName:str):
        index = Index("index",["collection.name"])
        indexService = IndexService(systemCollections["index"],index)
        data, lastKey = indexService.findDocuments({"collection":{"name":collectionName}})
        return list(map(lambda d: DynamoPlusService.buildIndex(collectionName+"#"+d["name"]), data))
    
    def getIndexConfigurationsByCollectionName(self, collectionName: str):
        documentTypeConfiguration = self.getSystemCollectionConfigurationFromCollectionName(collectionName)
        if documentTypeConfiguration:
            # should read the indexes from the environment variables
            indexesStrArray = self.systemIndexesStr.split(",")
            return list(filter(lambda i: i.documentType==collectionName, map(lambda x:  DynamoPlusService.buildIndex(x),indexesStrArray)))
        else:
            indexByDocType=next(filter(lambda i: "collection.name" in i.conditions, self.getIndexConfigurationsByCollectionName("index")))
            print("Index for doc type")
            print(indexByDocType.documentType)
            indexService = IndexService(self.getSystemCollectionConfigurationFromCollectionName("index"), indexByDocType)
            data, lastKey = indexService.findDocuments({"collection":{"name":collectionName}})
            print("Indexes by doc type")
            return list(map(lambda d: DynamoPlusService.buildIndex(collectionName+"#"+d["name"]), data))
            # for d in data:
            #     indexFound = 
            #     print(indexFound.conditions)
                # indexService = IndexService(self.getSystemCollectionConfigurationFromCollection("index"),indexConfiguration)
                # data, lastKey = indexService.findDocuments({"collection":{"name":documentType}})
        
                # if len(data)>0:
                #     print(data)
                
    @staticmethod
    def buildIndex(indexStr:str):
        indexStrArray = indexStr.split("#")
        documentType = indexStrArray[0]
        indexName = indexStrArray[1]
        parts1 = indexName.split("__ORDER_BY__")
        conditions=parts1[0].split("__")
        return Index(documentType,conditions,orderingKey=parts1[1] if len(parts1)>1 else None)
        
    def getIndexServiceByIndex(self, documentType:str, indexName:str):
        documentTypeConfiguration = self.getCollectionConfigurationFromCollectionName(documentType)
        if documentTypeConfiguration:
            index=None
            if indexName: 
                parts1 = indexName.split("__ORDER_BY__")
                conditions=parts1[0].split("__")
                index = Index(documentType,conditions,orderingKey=parts1[1] if len(parts1)>1 else None)
            return IndexService(documentTypeConfiguration,index)