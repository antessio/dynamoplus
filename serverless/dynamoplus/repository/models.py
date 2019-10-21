from typing import *
import logging
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.utils.utils import convertToString, findValue, getValuesByKeyRecursive
from dynamoplus.models.indexes.indexes import Index, Query, AbstractQuery, RangeQuery

logging.basicConfig(level=logging.INFO)


class QueryResult(object):
    def __init__(self, data:List['Model'], lastEvaluatedKey:dict=None):
        self.data = data
        self.lastEvaluatedKey=lastEvaluatedKey
        
class Model(object):
    def __init__(self, documentTypeConfiguration: DocumentTypeConfiguration,document:dict):
        self.idKey = documentTypeConfiguration.idKey
        self.orderKey = documentTypeConfiguration.orderingKey
        self.entityName = documentTypeConfiguration.entityName
        self.document = document        
    def pk(self):
        
        return self.document["pk"] if "pk" in self.document else  (self.entityName+"#"+self.document[self.idKey] if self.idKey in self.document else None)
    
    def sk(self):        
        return self.document["sk"] if "sk" in self.document else self.entityName
    
    def data(self):
        if "data" in self.document:
            return self.document["data"]
        else:
            data = convertToString(self.document[self.idKey])
            orderValue = self.orderValue()
            if orderValue:
                data = self.document[self.idKey]+"#"+orderValue
            return data
    def orderValue(self):
        if self.orderKey:
            return findValue(self.document, self.orderKey.split("."))
    def toDynamoDbItem(self):
        return {**self.document, "pk": self.pk(), "sk": self.sk(), "data": self.data()}

    def fromDynamoDbItem(self):
        return {k: v for k, v in self.document.items() if k not in ["pk","sk","data"]}

class IndexModel(Model):
    def __init__(self, documentTypeConfiguration, document, index:Index):
        super().__init__(documentTypeConfiguration, document)
        self.index = index
    def sk(self):
        if self.index is None:
            return self.entityName
        return self.entityName+"#"+"#".join(map(lambda x:x,self.index.conditions)) if self.index.conditions else self.entityName
    def data(self):
        if self.index is None:
            return None
        logging.info("orderKey {}".format(self.orderKey))
        orderValue = self.document[self.index.orderingKey] if self.index.orderingKey is not None and self.index.orderingKey in self.document else None
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
class IndexModelRange(Model):
    def __init__(self, documentTypeConfiguration, fromValue,toValue, index:Index):
        super().__init__(documentTypeConfiguration, None)
        self.index = index
        self.fromValue = fromValue
        self.toValue = toValue
    def sk(self):
        if self.index is None:
            return self.entityName
        return self.entityName+"#"+"#".join(map(lambda x:x,self.index.conditions)) if self.index.conditions else self.entityName
    def data(self):
        if self.index is None:
            return None
        logging.info("orderKey {}".format(self.orderKey))
        orderValue = self.document[self.index.orderingKey] if self.index.orderingKey is not None and self.index.orderingKey in self.document else None
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

class IndexModelFactory(object):
    @staticmethod
    def indexModelFromQuery(documentTypeConfiguration: DocumentTypeConfiguration, query:AbstractQuery):
        if type(query) is Query:
            return IndexModel(documentTypeConfiguration, query.document,query.index)
        elif type(query) is RangeQuery:
            return IndexModelRange(documentTypeConfiguration,query.fromValue, query.toValue, query.index)
        else:
            raise Exception("No implementation found")
        

