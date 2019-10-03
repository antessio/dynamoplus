import logging
import typing
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.service.IndexService import IndexService
logging.basicConfig(level=logging.INFO)


class ConfigurationService(object):
    def __init__(self, entityName:str, systemDocumentConfigurationsList:List[str]):
        self.entityName = entityName
        self.systemDocumentConfigurationsList=systemDocumentConfigurationsList

    def documentTypeConfiguration(self, targetEntity:str, dynamoDbTable, dynamoDB):
        '''
        If the system document type configuration is not found, search for custom entity configuration
        '''
        documentTypeConfiguration = self.systemDocumentTypeConfiguration(targetEntity)
        if not documentTypeConfiguration:
            documentTypeConfiguration = self.customDocumentTypeConfiguration(targetEntity,dynamoDbTable,dynamoDB)
        return documentTypeConfiguration
    def systemDocumentTypeConfiguration(self, targetEntity:str):
        targetConfigurationString = next(filter(lambda tc: tc.split("#")[0]==targetEntity, self.systemDocumentConfigurationsList),None)
        if targetConfigurationString:
            logging.info("Accessing to system entity {}".format(targetConfigurationString))
            targetConfigurationArray=targetConfigurationString.split("#")
            return DocumentTypeConfiguration(targetConfigurationArray[0],targetConfigurationArray[1], targetConfigurationArray[2] if len(targetConfigurationArray)>2 else None)
        else:
            return None
    def customDocumentTypeConfiguration(self, targetEntity:str,dynamoDbTable,dynamoDB):
        index = Index("document_type","name")
        systemDocumentTypesIndexService = IndexService(index,dynamoDbTable,dynamoDB)
        documentTypesResult = systemDocumentTypesIndexService.findByExample({"name": targetEntity})
        logging.info("Response is {}".format(str(documentTypesResult)))
        if len(documentTypesResult)>0:
            return DocumentTypeConfiguration(targetEntity,documentTypesResult[0].idKey(),documentTypesResult[0].orderingKey())
        else:
            return None