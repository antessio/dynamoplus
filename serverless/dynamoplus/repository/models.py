import json
from typing import *
import logging
import base64

from dynamoplus.models.query.conditions import Predicate, get_range_predicate, AnyMatch
from dynamoplus.models.system.collection.collection import Collection
from decimal import Decimal
from dynamoplus.models.query.query import Index
from dynamoplus.utils.decimalencoder import DecimalEncoder
from dynamoplus.utils.utils import convertToString, find_value, get_values_by_key_recursive
from dynamoplus.utils.utils import auto_str

logging.basicConfig(level=logging.INFO)


def getPk(document: dict, collectionName: str, idKey: str):
    return document["pk"] if "pk" in document else (
        collectionName + "#" + document[idKey] if idKey in document else None)


def getSk(document: dict, collectionName: str):
    return document["sk"] if "sk" in document else collectionName


def getData(document: dict, id_key: str, ordering_key: str = None):
    if "data" in document:
        return document["data"]
    else:
        if id_key in document:
            order_value = getOrderValue(document, ordering_key)
            if order_value:
                data = order_value
            elif "order_unique" in document:
                data = convertToString(document["order_unique"])
            else:
                data = convertToString(document[id_key])
            return data


def getOrderValue(document: dict, orderingKey: str):
    if orderingKey:
        return find_value(document, orderingKey.split("."))


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
                # return self.data == o.data
                return True
        else:
            return super().__eq__(o)


class Model(object):
    def __init__(self, collection: Collection, document: dict):
        self.idKey = collection.id_key
        self.ordering_key = collection.ordering_key
        self.collectionName = collection.name
        self.document = document

    def pk(self):
        return getPk(self.document, self.collectionName, self.idKey)

    def sk(self):
        return getSk(self.document, self.collectionName)

    def data(self):
        return getData(self.document, self.idKey, self.ordering_key)

    def order_value(self):
        return getOrderValue(self.document, self.ordering_key)

    def to_dynamo_db_item(self):
        return {"document": json.dumps(self.document, cls=DecimalEncoder), "pk": self.pk(), "sk": self.sk(),
                "data": self.data()}

    @staticmethod
    def from_dynamo_db_item(dynamo_db_item: dict, collection: Collection):
        if "document" in dynamo_db_item:
            return Model(collection, json.loads(dynamo_db_item["document"], parse_float=Decimal))

    def __str__(self) -> str:
        return "Model => collection_name = {} id_key = {} ordering_key = {} document = {}".format(self.collectionName,
                                                                                                  self.idKey,
                                                                                                  self.ordering_key,
                                                                                                  self.document)


class QueryModel(Model):
    def __init__(self, collection: Collection, index_fields:List[str], predicate: Predicate):
        self.predicate = predicate
        super().__init__(collection, None)
        self.collection = collection
        self.fields = index_fields
        self.values = self.predicate.get_values()

    def pk(self):
        return None

    def sk(self):
        return self.collection.name+("#{}".format("#".join(self.fields)) if self.fields is not None and  len(self.fields)>0 else "")

    def data(self):
        if self.predicate.is_range():
            range_predicate = get_range_predicate(self.predicate)
            data_prefix = ""
            non_range_values = list(filter(lambda v: range_predicate.to_value != v and range_predicate.from_value != v, self.values))
            if len(non_range_values) > 0:
                data_prefix = "#".join(non_range_values)
                data_prefix = data_prefix + "#"
            data1 = data_prefix + range_predicate.from_value
            data2 = data_prefix + range_predicate.to_value
            return data1, data2
        elif not isinstance(self.predicate, AnyMatch):
            return "#".join(self.values)


class IndexModel(Model):
    def __init__(self, collection: Collection, document: dict, index: Index):
        self.index = index
        super().__init__(collection, document)

    def use_begins_with(self):
        return len(self.index.conditions) < len(get_values_by_key_recursive(self.document, self.index.conditions))

    def start_from(self, start_from: str):
        return json.loads(base64.b64decode(start_from))
        # return {"sk": self.sk(), "data": self.data(), "pk": "{}#{}".format(self.collectionName,start_from)}

    def last_evaluated_key(self, dynamo_last_evaluated_key: dict):
        return str(base64.b64encode(bytes(json.dumps(dynamo_last_evaluated_key), "utf-8")), "utf-8")
        # return dynamo_last_evaluated_key["pk"].replace(self.collectionName+"#", "")

    def sk(self):
        if self.index is None:
            return self.collectionName
        if self.index.range_condition:
            return "{}#{}".format(self.collectionName, self.index.range_condition)
        return self.collectionName + "#" + "#".join(
            map(lambda x: x, self.index.conditions)) if self.index.conditions else self.collectionName

    def data(self):
        if self.index is None:
            return None
        logging.info("orderKey {}".format(self.index.ordering_key))
        order_value = None
        try:
            order_value = self.document[
                self.index.ordering_key] if self.index.ordering_key is not None and self.index.ordering_key in self.document else None
        except AttributeError:
            logging.debug("ordering key missing")
        logging.debug("orderingPart {}".format(order_value))
        logging.info("Entity {}".format(str(self.document)))
        if self.index.range_condition:
            v1, v2 = find_value(self.document, self.index.range_condition.split("."))
            return v1, v2
        elif self.index.conditions:
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

    @staticmethod
    def from_dynamo_db_item(dynamo_db_item: dict, collection: Collection, index: Index):
        if "document" in dynamo_db_item:
            return IndexModel(collection, json.loads(dynamo_db_item["document"], parse_float=Decimal), index)


@auto_str
class Query(object):

    def __init__(self, predicate: Predicate, collection: Collection, index_fields:List[str], limit: int = None, start_from: Model = None):
        self.start_from = start_from
        self.predicate = predicate
        self.collection = collection
        self.index_fields = index_fields
        self.limit = limit

    def get_model(self) -> QueryModel:
        return QueryModel(self.collection, self.index_fields, self.predicate)

    def __eq__(self, o: object) -> bool:
        if isinstance(o, Query):
            return self.predicate.__eq__(o.predicate) \
                   and self.collection.__eq__(o.collection) \
                   and self.start_from.__eq__(o.start_from) \
                   and self.limit.__eq__(o.limit)

        else:
            return super().__eq__(o)

    def __repr__(self) -> str:
        return self.__str__()
