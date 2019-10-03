from typing import *
import logging
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.utils.utils import convertToString, findValue, getValuesByKeyRecursive


logging.basicConfig(level=logging.INFO)

class Model(object):
    def __init__(self, documentTypeConfiguration: DocumentTypeConfiguration,document:dict):
        self.idKey = documentTypeConfiguration.idKey
        self.orderKey = documentTypeConfiguration.orderingKey
        self.entityName = documentTypeConfiguration.entityName
        self.document = document
    
    def pk(self):
        return self.entityName+"#"+self.document[self.idKey]
    
    def sk(self):        
        return self.entityName
    
    def data(self):
        data = convertToString(self.document[self.idKey])
        orderValue = self.orderValue()
        if orderValue:
            data = self.document[self.idKey]+"#"+orderValue
        return data
    def orderValue(self):
        if self.orderKey:
            return findValue(self.document, self.orderKey.split("."))

class IndexModel(Model):
    def __init__(self, documentTypeConfiguration, document, index:Index):
        super().__init__(documentTypeConfiguration, document)
        self.index = index
    def sk(self):        
        return self.entityName+"#"+"#".join(map(lambda x:x,self.index.conditions)) if self.index.conditions else self.entityName
    def data(self):
        logging.info("orderKey {}".format(self.orderKey))
        orderValue = self.orderValue()
        logging.debug("orderingPart {}".format(orderValue))
        logging.info("Entity {}".format(str(self.document)))
        if self.index.conditions:
            logging.info("Index keys {}".format(self.index.conditions))
            '''
                attr1#attr2#attr3#attr4#orderValue
            '''
            values = getValuesByKeyRecursive(self.document, self.index.conditions)
            logging.info("Found {} in conditions ".format(values))
            
            if values:
                data = "#".join(list(map(lambda v: convertToString(v), values)))
                if orderValue:
                    data = data+"#"+orderValue
                return data
            # keyPart = convertToString(getByKeyRecursive(entity,self.indexKeys,not query))
            # return keyPart+("#"+orderingPart if orderingPart is not None else "")
