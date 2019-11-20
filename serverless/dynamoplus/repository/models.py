from typing import *
import logging
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.query.query import Index
from dynamoplus.utils.utils import convertToString, find_value, get_values_by_key_recursive
## pynamodb
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, JSONAttribute
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection

##
import os

logging.basicConfig(level=logging.INFO)


def getPk(document: dict, collectionName: str, idKey: str):
    return document["pk"] if "pk" in document else (
        collectionName + "#" + document[idKey] if idKey in document else None)


def getSk(document: dict, collectionName: str):
    return document["sk"] if "sk" in document else collectionName


def getData(document: dict, idKey: str, orderingKey: str = None):
    if "data" in document:
        return document["data"]
    else:
        if idKey in document:
            data = convertToString(document[idKey])
            orderValue = getOrderValue(document, orderingKey)
            if orderValue:
                data = document[idKey] + "#" + orderValue
            return data


def getOrderValue(document: dict, orderingKey: str):
    if orderingKey:
        return find_value(document, orderingKey.split("."))


class IndexDataModel(GlobalSecondaryIndex):
    class Meta:
        # index_name is optional, but can be provided to override the default name
        index_name = 'sk-data-index'
        # All attributes are projected
        projection = AllProjection()
        read_capacity_units = 1
        write_capacity_units = 1

    sk = UnicodeAttribute(hash_key=True)
    data = UnicodeAttribute(range_key=True)

    # @staticmethod
    # def setup_model(model, region, table_name):
    #     model.Meta.table_name = table_name
    #     model.Meta.region = region


class SystemDataModel(Model):
    class Meta:
        table_name = os.environ.get('DYNAMODB_SYSTEM_TABLE')
        region = os.environ.get('REGION')

    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    data = UnicodeAttribute()
    document = JSONAttribute()
    skDataIndex = IndexDataModel()

    @staticmethod
    def setup_model(model, table_name, region):
        model.Meta.table_name = table_name
        if region:
            model.Meta.region = region


class DataModel(Model):
    class Meta:
        table_name = os.environ.get('DYNAMODB_DOMAIN_TABLE')
        region = os.environ.get('REGION')

    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    data = UnicodeAttribute()
    document = JSONAttribute()
    skDataIndex = IndexDataModel()

    @staticmethod
    def setup_model(model, table_name, region = None):
        model.Meta.table_name = table_name
        if region:
            model.Meta.region = region


class QueryResult(object):
    def __init__(self, data: List["Model"], last_evaluated_key: dict = None):
        """

        :type data: Model
        """
        self.data = data
        self.lastEvaluatedKey = last_evaluated_key

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return ".".join(map(lambda model: model.document, self.data))

    def __eq__(self, o: object) -> bool:
        if o is isinstance(QueryResult):
            if len(o.data) == len(self.data):
                #return self.data == o.data
                return True
        else:
            return super().__eq__(o)


class Model(object):
    def __init__(self, collection: Collection, document: dict, is_system: bool = False):
        self.idKey = collection.id_key
        self.ordering_key = collection.ordering_key
        self.collectionName = collection.name
        self.document = document
        if is_system:
            self.data_model_class = SystemDataModel
            self.data_model = SystemDataModel(self.pk(),self.sk(),data=self.data(),document=self.document)
            # self.data_model = SystemDataModel(getPk(self.document, self.collectionName, self.idKey),
            #                                   getSk(self.document, self.collectionName),
            #                                   data=getData(self.document, self.idKey, self.ordering_key),
            #                                   document=self.document)
        else:
            self.data_model_class = DataModel
            self.data_model = DataModel(self.pk(), self.sk(), data=self.data(), document=self.document)
            # self.data_model = DataModel(getPk(self.document, self.collectionName, self.idKey),
            #                             getSk(self.document, self.collectionName),
            #                             data=getData(self.document, self.idKey, self.ordering_key), document=self.document)

    def pk(self):
        return getPk(self.document, self.collectionName, self.idKey)

    def sk(self):
        return getSk(self.document, self.collectionName)

    def data(self):
        return getData(self.document, self.idKey, self.ordering_key)

    def order_value(self):
        return getOrderValue(self.document, self.ordering_key)

    def to_dynamo_db_item(self):
        return {**self.document, "pk": self.pk(), "sk": self.sk(), "data": self.data()}

    def from_dynamo_db_item(self):
        return {k: v for k, v in self.document.items() if k not in ["pk", "sk", "data"]}

    def __str__(self) -> str:
        return "Model => collection_name = {} id_key = {} ordering_key = {} document = {}".format(self.collectionName,self.idKey,self.ordering_key,self.document)


class IndexModel(Model):
    def __init__(self, collection:Collection, document:dict, index: Index, is_system: bool = False):
        self.index = index
        super().__init__(collection, document, is_system)

    def sk(self):
        if self.index is None:
            return self.collectionName
        return self.collectionName + "#" + "#".join(
            map(lambda x: x, self.index.conditions)) if self.index.conditions else self.collectionName

    def data(self):
        if self.index is None:
            return None
        logging.info("orderKey {}".format(self.ordering_key))
        order_value = None
        try:
            order_value = self.document[self.index.ordering_key] if self.index.ordering_key is not None and self.index.ordering_key in self.document else None
        except AttributeError:
            logging.debug("ordering key missing")
        logging.debug("orderingPart {}".format(order_value))
        logging.info("Entity {}".format(str(self.document)))
        if self.index.conditions:
            logging.info("Index keys {}".format(self.index.conditions))
            '''
                attr1#attr2#attr3#attr4#orderValue
            '''
            values = get_values_by_key_recursive(self.document, self.index.conditions)
            logging.info("Found {} in conditions ".format(values))

            if values:
                data = "#".join(list(map(lambda v: convertToString(v), values)))
                if order_value:
                    data = data + "#" + order_value
                return data
