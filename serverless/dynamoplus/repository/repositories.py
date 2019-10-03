from typing import *
import logging
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.utils.utils import convertToString, findValue


logging.basicConfig(level=logging.INFO)

class Model(object):
    def __init__(self,documentTypeConfiguration:DocumentTypeConfiguration, document:dict):
        self.idKey = documentTypeConfiguration.idKey
        self.orderKey = documentTypeConfiguration.orderingKey
        self.entityName = documentTypeConfiguration.entityName
        self.document = document
    
    def idKey(self):
        return self.entityName+"#"+self.document[self.idKey]
    
    def secondaryKey(self):        
        return self.entityName
    
    def data(self):
        data = convertToString(self.document[self.idKey])
        if self.orderKey is not None:
            orderValue = findValue(self.document, self.orderKey.split("."))
            if orderValue:
                data = self.document[self.idKey]+"#"+orderValue
        return data

# class IndexModel(Model):
#       def __init__(self,documentTypeConfiguration:DocumentTypeConfiguration, document:dict, index:Index):
#         # ## first element of the indexKeys is the entityPrefix 
#         super(IndexModel, self).__init__(documentTypeConfiguration,document)
#         self.index = index

        
    



# class Repository(object):
#     def __init__(self,documentTypeConfiguration:DocumentTypeConfiguration):
#         self.tableName = os.environ['DYNAMODB_TABLE']
#         self.dynamoDB = boto3.resource('dynamodb')
#         self.table = self.dynamoDB.Table(tableName)
#         self.primaryKey = documentTypeConfiguration.idKey
#         self.orderKey = documentTypeConfiguration.orderingKey
#         self.entityPrefix = documentTypeConfiguration.entityName