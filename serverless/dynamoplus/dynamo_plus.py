import abc
import logging

import uuid
from datetime import datetime
from enum import Enum

from dynamoplus.service.dynamoplus import DynamoPlusService
from dynamoplus.service.domain.domain import DomainService
from dynamoplus.service.system.system import SystemService, from_dict_to_collection, from_dict_to_index, \
    from_collection_to_dict, from_index_to_dict, from_dict_to_client_authorization, from_client_authorization_to_dict
from dynamoplus.service.validation_service import validate_collection, validate_index, validate_document, \
    validate_client_authorization

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

def get_all(collection_name:str, last_key:str, limit: int):
    is_system = DynamoPlusService.is_system(collection_name)
    last_evaluated_key = None
    if is_system:
        logger.info("Get {} metadata from system".format(collection_name))
        if collection_name == 'collection':
            last_collection_metadata = None
            collections, last_key = SystemService.get_all_collections(limit, last_key)
            documents = list(map(lambda c: from_collection_to_dict(c), collections))
            if last_key:
                logging.info("last evaluated key is {}".format(last_key))
                last_evaluated_key = last_key["pk"]
        else:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST, "{} not valid", collection_name)
    else:
        logger.info("get all  {} collection limit = {} last_key = {} ".format(collection_name, limit, last_key))
        collection_metadata = SystemService.get_collection_by_name(collection_name)
        if collection_metadata is None:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "{} is not a valid collection".format(collection_name))
        domain_service = DomainService(collection_metadata)
        logger.info("Query all {}".format(collection_name))
        documents, last_evaluated_key = domain_service.find_all(limit, last_key)
        return documents, last_evaluated_key


def get(collection_name: str, document_id: str):
    is_system = DynamoPlusService.is_system(collection_name)
    if is_system:
        logger.info("Get {} metadata from system".format(collection_name))
        if collection_name == 'collection':
            collection_metadata = SystemService.get_collection_by_name(document_id)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                       "{} not found with name {}".format(collection_name, document_id))
            logger.info("Found collection {}".format(collection_metadata.__str__))
            return collection_metadata.__dict__
        elif collection_name == 'index':
            index_metadata = SystemService.get_index(document_id, collection_name)
            if index_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                       "{} not found with name {}".format(collection_name, document_id))
            logger.info("Found index {}".format(index_metadata.__str__))
            return index_metadata.__dict__
        elif collection_name == 'client_authorization':
            client_authorization = SystemService.get_client_authorization(document_id)
            if client_authorization is None:
                raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                       "{} not found with name {}".format(collection_name, document_id))
            logger.info("Found client_authorization {}".format(client_authorization.__str__))
            return from_client_authorization_to_dict(client_authorization)
        else:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST, "{} not valid", collection_name)

    else:
        logger.info("Get {} document".format(collection_name))
        collection_metadata = SystemService.get_collection_by_name(collection_name)
        if collection_metadata is None:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "{} is not a valid collection".format(collection_name))
        document = DomainService(collection_metadata).get_document(document_id)
        if document is None:
            raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                   "{} not found with id {}".format(collection_name, document_id))
        return document


def create(collection_name: str, document: dict) -> dict:
    is_system = DynamoPlusService.is_system(collection_name)
    if is_system:
        logger.info("Creating {} metadata {}".format(collection_name, document))
        if collection_name == 'collection':
            validate_collection(document)
            collection_metadata = from_dict_to_collection(document)
            collection_metadata = SystemService.create_collection(collection_metadata)
            logger.info("Created collection {}".format(collection_metadata.__str__))
            return from_collection_to_dict(collection_metadata)
        elif collection_name == 'index':
            validate_index(document)
            index_metadata = from_dict_to_index(document)
            index_metadata = SystemService.create_index(index_metadata)
            logger.info("Created index {}".format(index_metadata.__str__))
            return from_index_to_dict(index_metadata)
        elif collection_name == 'client_authorization':
            validate_client_authorization(document)
            client_authorization = from_dict_to_client_authorization(document)
            client_authorization = SystemService.create_client_authorization(client_authorization)
            logging.info("created client_authorization {}".format(client_authorization.__str__()))
            return from_client_authorization_to_dict(client_authorization)
    else:
        logger.info("Create {} document {}".format(collection_name, document))
        collection_metadata = SystemService.get_collection_by_name(collection_name)
        validate_document(document, collection_metadata)
        if collection_metadata is None:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "{} is not a valid collection".format(collection_name))
        timestamp = datetime.utcnow()
        ## TODO: key generator
        if collection_metadata.id_key not in document:
            uid = str(uuid.uuid1())
            document[collection_metadata.id_key] = uid
        document["creation_date_time"] = timestamp.isoformat()
        d = DomainService(collection_metadata).create_document(document)
        logger.info("Created document {}".format(d))
        return d


def update(collection_name: str, document: dict):
    is_system = DynamoPlusService.is_system(collection_name)
    if is_system:
        if collection_name == "client_authorization":
            validate_client_authorization(document)
            client_authorization = from_dict_to_client_authorization(document)
            client_authorization = SystemService.update_authorization(client_authorization)
            logging.info("updated client_authorization {}".format(client_authorization.__str__))
            return from_client_authorization_to_dict(client_authorization)
        else:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "updating {} is not supported ".format(collection_name))
    else:
        logger.info("update {} document {}".format(collection_name, document))
        collection_metadata = SystemService.get_collection_by_name(collection_name)
        if collection_metadata is None:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "{} is not a valid collection".format(collection_name))
        validate_document(document,collection_metadata)
        timestamp = datetime.utcnow()
        document["update_date_time"] = timestamp.isoformat()
        d = DomainService(collection_metadata).update_document(document)
        logger.info("updated document {}".format(d))
        return d


def query(collection_name: str, query_id: str = None, example: dict = None, start_from: str = None,
          limit: int = None):
    is_system = DynamoPlusService.is_system(collection_name)
    documents = []
    last_evaluated_key = None
    if is_system:
        if collection_name == 'collection':
            collections, last_key = SystemService.get_all_collections(limit, start_from)
            documents = list(map(lambda c: from_collection_to_dict(c), collections))
            last_evaluated_key = last_key
        elif collection_name == 'index':
            index_metadata_list, last_key = SystemService.find_indexes_from_collection_name(
                example["collection"]["name"], limit, start_from)
            documents = list(map(lambda i: from_index_to_dict(i), index_metadata_list))
            last_evaluated_key = last_key
        else:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "{} is not a valid collection".format(collection_name))
    else:
        logger.info("query id {} collection {} by example ".format(query_id, collection_name, example))
        collection_metadata = SystemService.get_collection_by_name(collection_name)
        if collection_metadata is None:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "{} is not a valid collection".format(collection_name))
        domain_service = DomainService(collection_metadata)
        if query_id is None:
            logger.info("Query all {}".format(collection_name))
            documents, last_evaluated_key = domain_service.find_all(limit, start_from)
        else:
            index_metadata = SystemService.get_index(query_id, collection_name)
            if index_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST, "no index {} found".format(query_id))
            documents, last_evaluated_key = domain_service.find_by_index(index_metadata, example, limit, start_from)
    return documents, last_evaluated_key


def delete(collection_name: str, id: str):
    is_system = DynamoPlusService.is_system(collection_name)
    if is_system:
        logger.info("delete {} metadata from system".format(collection_name))
        if collection_name == 'collection':
            SystemService.delete_collection(id)
        elif collection_name == 'index':
            index_metadata = SystemService.delete_index(id)
        elif collection_name == 'client_authorization':
            SystemService.delete_authorization(id)
        else:
            raise NotImplementedError("collection_name {} not handled".format(collection_name))
    else:
        logger.info("delete {} document {}".format(collection_name, id))
        collection_metadata = SystemService.get_collection_by_name(collection_name)
        if collection_metadata is None:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "{} is not a valid collection".format(collection_name))
        DomainService(collection_metadata).delete_document(id)
