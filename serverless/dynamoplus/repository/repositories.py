from typing import *
import abc
import logging

from pynamodb.exceptions import DoesNotExist

from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.query.query import Index, Query
from dynamoplus.repository.models import Model, IndexModel, QueryResult, IndexDataModel, DataModel, SystemDataModel
from dynamoplus.utils.utils import convertToString, find_value, sanitize

import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

pynamoDbLogging = logging.getLogger("pynamodb")
pynamoDbLogging.setLevel(logging.DEBUG)
pynamoDbLogging.propagate = True


class Repository(abc.ABC):
    @abc.abstractmethod
    def getModelFromDocument(self, document: dict):
        pass

    @abc.abstractmethod
    def create(self, document: dict):
        pass

    @abc.abstractmethod
    def get(self, id: str):
        pass

    @abc.abstractmethod
    def update(self, document: dict):
        pass

    @abc.abstractmethod
    def delete(self, id: str):
        pass

    @abc.abstractmethod
    def find(self, query: Query):
        pass


class DynamoPlusRepository(Repository):
    def __init__(self, collection: Collection, isSystem=False):
        self.collection = collection
        ## TODO: table name depends if it's system or domain
        self.tableName = os.environ['DYNAMODB_DOMAIN_TABLE'] if not isSystem else os.environ['DYNAMODB_SYSTEM_TABLE']
        self.region = os.environ['REGION'] if 'REGION' in os.environ else 'eu-west-1'
        logger.info("Table name is {}".format(self.tableName))
        self.isSystem = isSystem
        if self.isSystem:
            SystemDataModel.setup_model(SystemDataModel, self.region, self.tableName)
        else:
            DataModel.setup_model(DataModel, self.region, self.tableName)

    def getModelFromDocument(self, document: dict):
        return Model(self.collection, document, self.isSystem)

    def create(self, document: dict):
        model = self.getModelFromDocument(document)
        response = model.data_model.save()
        # response = self.table.put_item(Item=sanitize(dynamoDbItem))
        logger.info("Response from put item operation is " + response.__str__())
        return model.data_model

    def get(self, id: str):
        # TODO: copy from query -> if the indexKeys is empty then get by primary key, otherwise get by global secondary index
        # it means if needed first get from index, then by primary key or, in case of index it throws a non supported operation exception
        model = self.getModelFromDocument({self.collection.id_key: id})
        logger.info("model for {} is {}".format(self.collection,model))
        try:
            return model.data_model.get(model.pk(), model.sk())
        except DoesNotExist as e:
            logger.error("{} doesn't exist ".format(id))
            logger.exception(e)

    def update(self, document: dict):
        model = self.getModelFromDocument(document)
        response = model.data_model.update(actions=[
            model.data_model_class.document.set(document)
        ])
        logger.info("Response from update operation is " + response.__str__())
        return model.data_model

    def delete(self, id: str):
        model = self.getModelFromDocument({self.collection.id_key: id})
        response = model.data_model.delete()

    def find(self, query: Query):
        return None


class IndexDynamoPlusRepository(DynamoPlusRepository):
    def __init__(self, collection: Collection, index: Index, isSystem=False):
        super().__init__(collection, isSystem)
        self.index = index

    def getModelFromDocument(self, document: dict):
        return IndexModel(self.collection, document, self.index, self.isSystem)

    def find(self, query: Query):
        logger.info(" Received query={}".format(query.__str__()))
        document = query.document
        index_model = IndexModel(self.collection, document, query.index,self.isSystem)
        ordering_key = None
        try:
            query.index.ordering_key if query.index else None
        except AttributeError:
            logger.info("missing ordering key in index")
        logger.info("order by is {} ".format(ordering_key))
        limit = query.limit
        start_from = query.start_from
        result = []
        index_model.data_model.skDataIndex.Meta.table_name = self.tableName
        index_model.data_model.skDataIndex.Meta.model.Meta.table_name = self.tableName
        if index_model.data() is not None:
            if ordering_key is None:
                logger.info(
                    "The key that will be used is sk={} is equal data={}".format(index_model.sk(), index_model.data()))
                result = index_model.data_model.skDataIndex.query(index_model.sk(),
                                                                  index_model.data_model_class.data == index_model.data())
            else:
                logger.info(
                    "The key that will be used is sk={} begins with data={}".format(index_model.sk(),
                                                                                    index_model.data()))
                result = index_model.data_model.skDataIndex.query(index_model.sk(),
                                                                  index_model.data_model_class.data.startswith(
                                                                     index_model.data()))
        else:
            logger.info("The key that will be used is sk={} with no data".format(index_model.sk()))
            result = index_model.data_model.skDataIndex.query(index_model.sk())

        last_key = result.last_evaluated_key if result.last_evaluated_key else None
        items = []
        for i in result:
            items.append(Model(self.collection, i.document))
        return QueryResult(items, last_key)
