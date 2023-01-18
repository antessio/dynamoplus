import abc
import logging

import uuid
from datetime import datetime
import time
from enum import Enum

import dynamoplus.v2.service.system.system_service_v2
from dynamoplus.models.system.aggregation.aggregation import AggregationJoin, AggregationTrigger, AggregationType
from dynamoplus.models.system.collection.collection import AttributeDefinition
from dynamoplus.models.system.index.index import IndexConfiguration
from dynamoplus.v2.service.query_service import QueryService
from dynamoplus.v2.service.system.system_service import CollectionService, IndexService, \
    AuthorizationService, Converter, Collection
from dynamoplus.models.query.conditions import Predicate, Range, Eq, And
from dynamoplus.v2.service.domain.domain_service import DomainService
from dynamoplus.v2.service.common import is_system
from dynamoplus.service.validation_service import validate_collection, validate_index, validate_document, \
    validate_client_authorization, validate_aggregation
from dynamoplus.service.indexing_decorator import create_document, update_document, delete_document
from dynamoplus.v2.service.system.system_service_v2 import AggregationConfigurationService, AggregationConfiguration, \
    Aggregation, AggregationCount, AggregationSum, AggregationAvg, AggregationService

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class HandlerExceptionErrorCodes(Enum):
    BAD_REQUEST = 400
    INTERNAL_SERVER_ERROR = 500
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404


class HandlerException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message


def from_API_to_aggregation_configuration(document: dict):
    collection_name = document["collection"]["name"]
    t = AggregationType.value_of(document["type"])
    inner_aggregation_document = document["configuration"]
    on = list(map(lambda o: AggregationTrigger.value_of(o), inner_aggregation_document["on"]))
    target_field = None
    matches = None
    join = None
    if "target_field" in inner_aggregation_document:
        target_field = inner_aggregation_document["target_field"]
    if "join" in inner_aggregation_document:
        join = AggregationJoin(inner_aggregation_document["join"]["collection_name"],
                               inner_aggregation_document["join"]["using_field"])
    if "matches" in inner_aggregation_document:
        matches = Converter.from_dict_to_predicate(inner_aggregation_document["matches"])

    return AggregationConfiguration(uuid.uuid4(), collection_name, t, on, target_field, matches, join)


def from_aggregation_configuration_to_API(aggregation_configuration: AggregationConfiguration,
                                          aggregation: Aggregation = None):
    a = {
        "id": str(aggregation_configuration.uid),
        "on": list(map(lambda o: o.name, aggregation_configuration.on))
    }
    d = {
        "collection": {
            "name": aggregation_configuration.collection_name
        },
        "type": aggregation_configuration.type.name
    }
    if aggregation_configuration.join:
        a["join"] = {
            "collection_name": aggregation_configuration.join.collection_name,
            "using_field": aggregation_configuration.join.using_field
        }
    if aggregation_configuration.target_field:
        a["target_field"] = aggregation_configuration.target_field
    if aggregation_configuration.matches:
        a["matches"] = Converter.from_predicate_to_dict(aggregation_configuration.matches)
    d["configuration"] = a
    d["name"] = aggregation_configuration.name
    if aggregation:
        d["aggregation"] = from_aggregation_to_API(aggregation)
    return d


def from_aggregation_to_API(aggregation: Aggregation):
    a = {
        "id": str(aggregation.id),
        "name": aggregation.name
    }
    if isinstance(aggregation, AggregationCount):
        a["type"] = AggregationType.COLLECTION_COUNT.name
        a["payload"] = {
            "count": int(aggregation.count)
        }
    if isinstance(aggregation, AggregationSum):
        a["type"] = AggregationType.SUM.name
        a["payload"] = {
            "sum": int(aggregation.sum)
        }
    if isinstance(aggregation, AggregationAvg):
        a["type"] = AggregationType.AVG.name
        a["payload"] = {
            "avg": aggregation.avg
        }
    return a


def from_collection_to_API(collection: dynamoplus.v2.service.system.system_service_v2.Collection) -> dict:
    result = {"name": collection.name, "id_key": collection.id_key,
              "auto_generated_id": True if collection.auto_generated_id else False}
    if collection.ordering:
        result["ordering_key"] = collection.ordering
    if collection.attributes:
        result["attributes"] = list(map(lambda a: from_attribute_definition_to_API(a), collection.attributes))

    return result


def from_index_to_API(index: dynamoplus.v2.service.system.system_service_v2.Index) -> dict:
    return {
        "id": str(index.id),
        "name": index.name,
        "configuration": index.index_configuration.name,
        "collection": {
            "name": index.collection_name
        },
        "conditions": index.conditions,
        "ordering_key": index.ordering_key
    }


def from_dict_to_index(d: dict):
    return dynamoplus.v2.service.system.system_service_v2.Index(
        uuid.uuid4(),
        d["collection"]["name"],
        d["conditions"],
        IndexConfiguration.value_of(d["configuration"]) if "configuration" in d else None,
        d["ordering_key"] if "ordering_key" in d else None)


def from_attribute_definition_to_API(attribute_definition: AttributeDefinition):
    result = {"name": attribute_definition.name, "type": attribute_definition.type.name}
    if attribute_definition.attributes:
        result["attributes"] = list(map(lambda a: from_attribute_definition_to_API(a), attribute_definition.attributes))
    return result


def from_dict_to_collection(d: dict):
    attributes = list(
        map(Converter.from_dict_to_attribute_definition, d["attributes"])) if "attributes" in d else None
    auto_generate_id = d["auto_generate_id"] if "auto_generate_id" in d else False
    return dynamoplus.v2.service.system.system_service_v2.Collection(d["name"], d["id_key"],
                                                                     d["ordering"] if "ordering" in d else None,
                                                                     attributes,
                                                                     auto_generate_id)


class Dynamoplus:

    def __init__(self):
        self.aggregation_configuration_service = AggregationConfigurationService()
        self.aggregation_service = AggregationService()
        self.index_service = dynamoplus.v2.service.system.system_service_v2.IndexService()
        self.collection_service = dynamoplus.v2.service.system.system_service_v2.CollectionService()
        self.client_authorization_service = dynamoplus.v2.service.system.system_service_v2.AuthorizationService()

    def get_all(self, collection_name: str, last_key: str, limit: int):
        is_system_collection = is_system(Collection(collection_name, None))
        if is_system_collection:
            logger.info("Get {} metadata from system".format(collection_name))
            if collection_name == 'collection':

                collections, last_evaluated_key = self.collection_service.get_all_collections(limit, last_key)
                documents = list(map(lambda c: from_collection_to_API(c), collections))
                return documents, last_evaluated_key
            elif collection_name == 'aggregation_configuration':
                aggregation_configuration_list, last_evaluated_key = self.aggregation_configuration_service.get_all_aggregation_configurations(
                    limit, uuid.UUID(last_key) if last_key else None)
                documents = list(map(lambda c: from_aggregation_configuration_to_API(c,
                                                                                     self.aggregation_service.get_aggregation_by_configuration_name(
                                                                                         c.name)),
                                     aggregation_configuration_list))
                return documents, last_evaluated_key
            elif collection_name == 'aggregation':
                aggregations, last_evaluated_key = self.aggregation_service.get_all_aggregations(limit, uuid.UUID(
                    last_key) if last_key else None)
                documents = list(map(lambda c: from_aggregation_to_API(c), aggregations))
                return documents, last_evaluated_key
            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} not valid collection".format(collection_name))
        else:
            logger.info("get all  {} collection limit = {} last_key = {} ".format(collection_name, limit, last_key))
            collection_metadata = CollectionService.get_collection(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            domain_service = DomainService(collection_metadata)
            logger.info("Query all {}".format(collection_name))
            documents, last_evaluated_key = domain_service.find_all(limit, last_key)
            return documents, last_evaluated_key

    def aggregation_configurations(self, collection_name: str, last_key: str, limit: int):
        is_system_collection = is_system(Collection(collection_name, None))
        if is_system_collection:
            raise HandlerException(HandlerExceptionErrorCodes.FORBIDDEN,
                                   "cannot get aggregation for system collections {}".format(collection_name))
        else:
            aggregation_configurations, last_evaluated_key = self.aggregation_configuration_service.get_aggregation_configurations_by_collection_name(
                collection_name, limit, uuid.UUID(last_key) if last_key else None)
            documents = list(map(lambda c: from_aggregation_configuration_to_API(c,
                                                                                 self.aggregation_service.get_aggregation_by_configuration_name(
                                                                                     c.name)),
                                 aggregation_configurations))
            return documents, last_evaluated_key

    def get(self, collection_name: str, document_id: str):
        is_system_collection = is_system(Collection(collection_name, None))
        if is_system_collection:
            logger.info("Get {} metadata from system".format(collection_name))
            if collection_name == 'collection':
                collection = self.collection_service.get_collection(document_id)
                if collection is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, document_id))
                logger.info("Found collection {}".format(collection.__str__))
                return from_collection_to_API(collection)
            elif collection_name == 'index':
                index = self.index_service.get_index_by_id(uuid.UUID(document_id))

                if index is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, document_id))
                logger.info("Found index {}".format(index.__str__))
                return from_index_to_API(index)
            elif collection_name == 'client_authorization':

                client_authorization = AuthorizationService.get_client_authorization(document_id)
                if client_authorization is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, document_id))
                logger.info("Found client_authorization {}".format(client_authorization.__str__))
                return Converter.from_client_authorization_to_dict(client_authorization)
            elif collection_name == 'aggregation_configuration':
                aggregation_configuration = self.aggregation_configuration_service.get_aggregation_configuration_by_uid(
                    uuid.UUID(document_id))
                if aggregation_configuration is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, document_id))
                logger.info("Found aggregation configuration {}".format(aggregation_configuration.__str__))
                return from_aggregation_configuration_to_API(aggregation_configuration)
            elif collection_name == 'aggregation':
                aggregation = self.aggregation_service.get_aggregation_by_id(uuid.UUID(document_id))
                if aggregation is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, document_id))
                logger.info("Found aggregation {}".format(aggregation.__str__))
                return from_aggregation_to_API(aggregation)
            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} not a valid collection".format(collection_name))

        else:
            logger.info("Get {} document".format(collection_name))
            collection = CollectionService.get_collection(collection_name)
            if collection is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            document = DomainService(collection).get_document(document_id)
            if document is None:
                raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                       "{} not found with id {}".format(collection_name, document_id))
            return document

    @create_document
    def create(self, collection_name: str, document: dict) -> dict:
        is_system_collection = is_system(Collection(collection_name, None))
        if is_system_collection:
            logger.info("Creating {} metadata {}".format(collection_name, document))
            if collection_name == 'collection':
                validate_collection(document)
                collection = self.collection_service.create_collection(from_dict_to_collection(document))
                logger.info("Created collection {}".format(collection.__str__))
                return from_collection_to_API(collection)
            elif collection_name == 'index':
                validate_index(document)
                index = from_dict_to_index(document)
                index = self.index_service.create_index(index)
                logger.info("Created index {}".format(index))
                return from_index_to_API(index)
            elif collection_name == 'client_authorization':
                validate_client_authorization(document)
                client_authorization = Converter.from_dict_to_client_authorization(document)
                client_authorization = AuthorizationService.create_client_authorization(client_authorization)
                logging.info("created client_authorization {}".format(client_authorization.__str__()))
                return Converter.from_client_authorization_to_dict(client_authorization)
            elif collection_name == "aggregation_configuration":
                validate_aggregation(document)
                aggregation = from_API_to_aggregation_configuration(document)
                aggregation = self.aggregation_configuration_service.create_aggregation_configuration(aggregation)
                logging.info("created aggregation {}".format(aggregation.__str__()))
                return Converter.from_aggregation_configuration_to_API(aggregation)
            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
        else:
            logger.info("Create {} document {}".format(collection_name, document))
            collection = CollectionService.get_collection(collection_name)
            if collection is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            validate_document(document, collection)
            timestamp = datetime.utcnow()
            ## TODO: key generator
            if collection.auto_generate_id:
                document[collection.id_key] = str(uuid.uuid1())
            document["creation_date_time"] = timestamp.isoformat()
            document["order_unique"] = str(int(time.time() * 1000.0))
            d = DomainService(collection).create_document(document)
            logger.info("Created document {}".format(d))
            return d

    @update_document
    def update(self, collection_name: str, document: dict, document_id: str = None):
        is_system_collection = is_system(Collection(collection_name, None))
        if is_system_collection:
            if collection_name == "client_authorization":
                if document_id:
                    document["client_id"] = document_id
                validate_client_authorization(document)
                client_authorization = Converter.from_dict_to_client_authorization(document)
                client_authorization = AuthorizationService.update_authorization(client_authorization)
                logging.info("updated client_authorization {}".format(client_authorization.__str__))
                return Converter.from_client_authorization_to_dict(client_authorization)
            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "updating {} is not supported ".format(collection_name))
        else:
            logger.info("update {} document {}".format(collection_name, document))
            collection_metadata = CollectionService.get_collection(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            if document_id:
                document[collection_metadata.id_key] = document_id
            validate_document(document, collection_metadata)
            timestamp = datetime.utcnow()
            document["update_date_time"] = timestamp.isoformat()
            d = DomainService(collection_metadata).update_document(document)
            logger.info("updated document {}".format(d))
            return d

    def query(self, collection_name: str, query: dict = None, start_from: str = None,
              limit: int = None):
        is_system_collection = is_system(Collection(collection_name, None))
        documents = []
        if is_system_collection:
            if collection_name == 'collection':
                collections, last_key = self.collection_service.get_all_collections(limit, start_from)
                documents = list(map(lambda c: from_collection_to_API(c), collections))
                last_evaluated_key = last_key
            elif collection_name == 'index' and "matches" in query and "eq" in query["matches"] and "value" in \
                    query["matches"]["eq"]:
                target_collection_name = query["matches"]["eq"]["value"]
                index_metadata_list, last_key = IndexService.get_index_by_collection_name(
                    target_collection_name, limit, start_from)
                documents = list(map(lambda i: Converter.from_index_to_dict(i), index_metadata_list))
                last_evaluated_key = last_key
            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
        else:
            if "matches" in query:
                predicate: Predicate = Converter.from_dict_to_predicate(query["matches"])
            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "invalid predicate")
            logger.info("query {} collection by {} ".format(collection_name, predicate))
            collection_metadata = CollectionService.get_collection(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            domain_service = DomainService(collection_metadata)
            ## TODO - missing order unique in the query
            query_id = "__".join(predicate.get_fields())
            index_matching_conditions = self.index_service.get_index_by_collection_name_and_conditions(collection_name,
                                                                                                       predicate.get_fields())
            # index_matching_conditions = IndexService.get_index_matching_fields(predicate.get_fields(), collection_name,
            #                                                                    None)
            logger.info("Found index matching {}".format(index_matching_conditions.conditions))
            if index_matching_conditions is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST, "no index {} found".format(query_id))
            ## Since the sk should be built using the index it is necessary to pass the index matching the conditions
            result = QueryService.query(collection_metadata, predicate, index_matching_conditions, start_from, limit)
            documents = list(map(lambda m: m.document, result.data))
            last_evaluated_key = result.lastEvaluatedKey
        return documents, last_evaluated_key

    @delete_document
    def delete(self, collection_name: str, id: str):
        is_system_collection = is_system(Collection(collection_name, None))
        if is_system_collection:
            logger.info("delete {} metadata from system".format(collection_name))
            if collection_name == 'collection':
                self.collection_service.delete_collection(id)
            elif collection_name == 'index':
                index_metadata = self.index_service.delete_index(uuid.UUID(id))
            elif collection_name == 'client_authorization':
                AuthorizationService.delete_authorization(id)
            else:
                raise NotImplementedError("collection_name {} not handled".format(collection_name))
        else:
            logger.info("delete {} document {}".format(collection_name, id))
            collection_metadata = CollectionService.get_collection(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            DomainService(collection_metadata).delete_document(id)
