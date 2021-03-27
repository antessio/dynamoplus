import abc
import logging

import uuid
from datetime import datetime
import time
from enum import Enum

from dynamoplus.v2.service.query_service import QueryService
from dynamoplus.v2.service.system.system_service import CollectionService, IndexService, \
    AuthorizationService, Converter, Collection,AggregationService
from dynamoplus.models.query.conditions import Predicate, Range, Eq, And
from dynamoplus.v2.service.domain.domain_service import DomainService
from dynamoplus.v2.service.common import is_system
from dynamoplus.service.validation_service import validate_collection, validate_index, validate_document, \
    validate_client_authorization, validate_aggregation
from dynamoplus.service.indexing_decorator import create_document, update_document, delete_document

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


def from_predicate_to_dict(predicate: Predicate):
    if isinstance(predicate, Eq):
        return {"eq": {"field_name": predicate.field_name, "value": predicate.value}}
    elif isinstance(predicate, Range):
        return {"range": {"field_name": predicate.field_name, "from": predicate.from_value, "to": predicate.to_value}}
    elif isinstance(predicate, And):
        return {"and": list(map(lambda c: from_predicate_to_dict(c), predicate.conditions))}


def from_dict_to_predicate(d: dict):
    if "eq" in d:
        return Eq(d["eq"]["field_name"], d["eq"]["value"])
    elif "range" in d:
        return Range(d["range"]["field_name"], d["range"]["from"], d["range"]["to"])
    elif "and" in d:
        conditions = list(map(lambda cd: from_dict_to_predicate(cd), d["and"]))
        return And(conditions)


def get_all(collection_name: str, last_key: str, limit: int):
    is_system_collection = is_system(Collection(collection_name, None))
    last_evaluated_key = None
    if is_system_collection:
        logger.info("Get {} metadata from system".format(collection_name))
        if collection_name == 'collection':
            last_collection_metadata = None
            collections, last_evaluated_key = CollectionService.get_all_collections(limit, last_key)
            documents = list(map(lambda c: Converter.from_collection_to_dict(c), collections))
            return documents, last_evaluated_key
        elif collection_name == 'aggregation':
            aggregations, last_evaluated_key = AggregationService.get_all_aggregations(limit, last_key)
            documents = list(map(lambda c: Converter.from_aggregation_to_dict(c), aggregations))
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


def get(collection_name: str, document_id: str):
    is_system_collection = is_system(Collection(collection_name, None))
    if is_system_collection:
        logger.info("Get {} metadata from system".format(collection_name))
        if collection_name == 'collection':
            collection_metadata = CollectionService.get_collection(document_id)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                       "{} not found with name {}".format(collection_name, document_id))
            logger.info("Found collection {}".format(collection_metadata.__str__))
            return collection_metadata.__dict__
        elif collection_name == 'index':
            index_metadata = IndexService.get_index_by_name_and_collection_name(document_id, collection_name)
            if index_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                       "{} not found with name {}".format(collection_name, document_id))
            logger.info("Found index {}".format(index_metadata.__str__))
            return index_metadata.__dict__
        elif collection_name == 'client_authorization':

            client_authorization = AuthorizationService.get_client_authorization(document_id)
            if client_authorization is None:
                raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                       "{} not found with name {}".format(collection_name, document_id))
            logger.info("Found client_authorization {}".format(client_authorization.__str__))
            return Converter.from_client_authorization_to_dict(client_authorization)
        elif collection_name == 'aggregation':
            aggregation = AggregationService.get_aggregation_by_name(document_id)
            if aggregation is None:
                raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                       "{} not found with name {}".format(collection_name, document_id))
            logger.info("Found aggregation {}".format(aggregation.__str__))
            return Converter.from_aggregation_to_dict(aggregation)
        else:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST, "{} not a valid collection", collection_name)

    else:
        logger.info("Get {} document".format(collection_name))
        collection_metadata = CollectionService.get_collection(collection_name)
        if collection_metadata is None:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "{} is not a valid collection".format(collection_name))
        document = DomainService(collection_metadata).get_document(document_id)
        if document is None:
            raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                   "{} not found with id {}".format(collection_name, document_id))
        return document


@create_document
def create(collection_name: str, document: dict) -> dict:
    is_system_collection = is_system(Collection(collection_name, None))
    if is_system_collection:
        logger.info("Creating {} metadata {}".format(collection_name, document))
        if collection_name == 'collection':
            validate_collection(document)
            collection_metadata = Converter.from_dict_to_collection(document)
            collection_metadata = CollectionService.create_collection(collection_metadata)
            logger.info("Created collection {}".format(collection_metadata.__str__))
            return Converter.from_collection_to_dict(collection_metadata)
        elif collection_name == 'index':
            validate_index(document)
            index_metadata = Converter.from_dict_to_index(document)
            index_metadata = IndexService.create_index(index_metadata)
            logger.info("Created index {}".format(index_metadata.__str__()))
            return Converter.from_index_to_dict(index_metadata)
        elif collection_name == 'client_authorization':
            validate_client_authorization(document)
            client_authorization = Converter.from_dict_to_client_authorization(document)
            client_authorization = AuthorizationService.create_client_authorization(client_authorization)
            logging.info("created client_authorization {}".format(client_authorization.__str__()))
            return Converter.from_client_authorization_to_dict(client_authorization)
        elif collection_name == "aggregation":
            validate_aggregation(document)
            aggregation = Converter.from_dict_to_aggregation(document)
            aggregation = AggregationService.create_aggregation(aggregation)
            logging.info("created aggregation {}".format(aggregation.__str__()))
            return Converter.from_aggregation_to_dict(aggregation)
        else:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "{} is not a valid collection".format(collection_name))
    else:
        logger.info("Create {} document {}".format(collection_name, document))
        collection_metadata = CollectionService.get_collection(collection_name)
        if collection_metadata is None:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "{} is not a valid collection".format(collection_name))
        validate_document(document, collection_metadata)
        timestamp = datetime.utcnow()
        ## TODO: key generator
        if collection_metadata.auto_generate_id:
            document[collection_metadata.id_key] = str(uuid.uuid1())
        document["creation_date_time"] = timestamp.isoformat()
        document["order_unique"] = str(int(time.time() * 1000.0))
        d = DomainService(collection_metadata).create_document(document)
        logger.info("Created document {}".format(d))
        return d

@update_document
def update(collection_name: str, document: dict, document_id: str = None):
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


def query(collection_name: str, query: dict = None, start_from: str = None,
          limit: int = None):
    is_system_collection = is_system(Collection(collection_name, None))
    documents = []
    if is_system_collection:
        if collection_name == 'collection':
            collections, last_key = CollectionService.get_all_collections(limit, start_from)
            documents = list(map(lambda c: Converter.from_collection_to_dict(c), collections))
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
            predicate: Predicate = from_dict_to_predicate(query["matches"])
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
        index_matching_conditions = IndexService.get_index_matching_fields(predicate.get_fields(), collection_name,
                                                                           None)
        logger.info("Found index matching {}".format(index_matching_conditions.conditions))
        if index_matching_conditions is None:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST, "no index {} found".format(query_id))
        ## Since the sk should be built using the index it is necessary to pass the index matching the conditions
        result = QueryService.query(collection_metadata, predicate, index_matching_conditions, start_from, limit)
        documents = list(map(lambda m: m.document, result.data))
        last_evaluated_key = result.lastEvaluatedKey
    return documents, last_evaluated_key

@delete_document
def delete(collection_name: str, id: str):
    is_system_collection = is_system(Collection(collection_name, None))
    if is_system_collection:
        logger.info("delete {} metadata from system".format(collection_name))
        if collection_name == 'collection':
            CollectionService.delete_collection(id)
        elif collection_name == 'index':
            index_metadata = IndexService.delete_index(id)
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
