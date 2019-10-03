import logging
from typing import *
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.service.indexes import IndexService
logging.basicConfig(level=logging.INFO)


class ConfigurationService(object):
    def __init__(self, entityName:str, systemDocumentConfigurationsList:List[str]):
        self.entityName = entityName
        self.systemDocumentConfigurationsList=systemDocumentConfigurationsList

    def documentTypeConfiguration(self, targetEntity:str):
        documentTypeConfiguration = self.systemDocumentTypeConfiguration(targetEntity)
        if not documentTypeConfiguration:
            documentTypeConfiguration = self.customDocumentTypeConfiguration(targetEntity)
        return documentTypeConfiguration
    def systemDocumentTypeConfiguration(self, targetEntity:str):
        targetConfigurationString = next(filter(lambda tc: tc.split("#")[0]==targetEntity, self.systemDocumentConfigurationsList),None)
        if targetConfigurationString:
            logging.info("Accessing to system entity {}".format(targetConfigurationString))
            targetConfigurationArray=targetConfigurationString.split("#")
            return DocumentTypeConfiguration(targetConfigurationArray[0],targetConfigurationArray[1], targetConfigurationArray[2] if len(targetConfigurationArray)>2 else None)
        else:
            return None
    def customDocumentTypeConfiguration(self, targetEntity:str):
        index = Index("document_type","name")
        systemDocumentTypesIndexService = IndexService(index)
        documentTypesResult = systemDocumentTypesIndexService.findByExample({"name": targetEntity})
        logging.info("Response is {}".format(str(documentTypesResult)))
        if len(documentTypesResult)>0:
            return DocumentTypeConfiguration(targetEntity,documentTypesResult[0]["idKey"], documentTypesResult[0]["orderingKey"])
        else:
            return None