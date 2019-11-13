from typing import *
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.service.indexes import IndexService
from dynamoplus.repository.repositories import DynamoPlusRepository
from dynamoplus.models.indexes.indexes import Query, Index

# from dynamoplus.repository.models import QueryResult, Document

collectionMetadata = Collection("collection", "name")
indexMetadata = Collection("index", "name")
systemCollections = {
    "collection": collectionMetadata,
    "index": indexMetadata
}


class DynamoPlusService(object):
    def __init__(self):
        pass

    def getSystemCollectionConfigurationFromCollectionName(self, systemCollectionName: str):
        if systemCollectionName in systemCollections:
            return systemCollections[systemCollectionName]

    def getCustomCollectionConfigurationFromCollectionName(self, documentType: str):
        collection = self.getSystemCollectionConfigurationFromCollectionName("collection")
        index = Index(documentType, ["name"])
        query = Query({"name": documentType}, index)
        data, lastKey = IndexService(collection, index).find_documents({"name": documentType})
        if len(data) > 0:
            documenTypeDict = data[0]
            return Collection(documentType, documenTypeDict["idKey"], documenTypeDict["orderingKey"])

    def getCollectionConfigurationFromCollectionName(self, collectionName: str):
        documentTypeConfiguration = self.getSystemCollectionConfigurationFromCollectionName(collectionName)
        if documentTypeConfiguration:
            return documentTypeConfiguration
        else:
            return self.getCustomCollectionConfigurationFromCollectionName(collectionName)

    def get_indexes_from_collecion_name(self, collection_name: str):
        index = Index("index", ["collection.name"])
        index_service = IndexService(systemCollections["index"], index, True)
        data, last_key = index_service.find_documents({"collection": {"name": collection_name}})
        return list(map(lambda d: DynamoPlusService.buildIndex(collection_name + "#" + d["name"]), data))

    def getIndexConfigurationsByCollectionName(self, collectionName: str):
        documentTypeConfiguration = self.getSystemCollectionConfigurationFromCollectionName(collectionName)
        if documentTypeConfiguration:
            # should read the indexes from the environment variables
            indexesStrArray = self.systemIndexesStr.split(",")
            return list(filter(lambda i: i.collection_name == collectionName,
                               map(lambda x: DynamoPlusService.buildIndex(x), indexesStrArray)))
        else:
            indexByDocType = next(filter(lambda i: "collection.name" in i.conditions,
                                         self.getIndexConfigurationsByCollectionName("index")))
            print("Index for doc type")
            print(indexByDocType.collection_name)
            indexService = IndexService(self.getSystemCollectionConfigurationFromCollectionName("index"),
                                        indexByDocType)
            data, lastKey = indexService.find_documents({"collection": {"name": collectionName}})
            print("Indexes by doc type")
            return list(map(lambda d: DynamoPlusService.buildIndex(collectionName + "#" + d["name"]), data))
            # for d in data:
            #     indexFound = 
            #     print(indexFound.conditions)
            # indexService = IndexService(self.getSystemCollectionConfigurationFromCollection("index"),indexConfiguration)
            # data, lastKey = indexService.findDocuments({"collection":{"name":documentType}})

            # if len(data)>0:
            #     print(data)

    @staticmethod
    def buildIndex(indexStr: str):
        indexStrArray = indexStr.split("#")
        documentType = indexStrArray[0]
        indexName = indexStrArray[1]
        parts1 = indexName.split("__ORDER_BY__")
        conditions = parts1[0].split("__")
        return Index(documentType, conditions, ordering_key=parts1[1] if len(parts1) > 1 else None)

    def getIndexServiceByIndex(self, documentType: str, indexName: str):
        documentTypeConfiguration = self.getCollectionConfigurationFromCollectionName(documentType)
        if documentTypeConfiguration:
            index = None
            if indexName:
                parts1 = indexName.split("__ORDER_BY__")
                conditions = parts1[0].split("__")
                index = Index(documentType, conditions, ordering_key=parts1[1] if len(parts1) > 1 else None)
            return IndexService(documentTypeConfiguration, index)
