import abc
import logging
import os
import uuid
from datetime import datetime
from enum import Enum

from dynamoplus.service.domain.domain import DomainService
from dynamoplus.service.system.system import SystemService, from_dict_to_collection, from_dict_to_index,from_collection_to_dict,from_index_to_dict

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


class DynamoPlusHandlerInterface(abc.ABC):

    @abc.abstractmethod
    def get(self, collection_name: str, id: str):
        '''
        1)
        domain:
            if collectioName not found system:
                raise NotFoundException
        system:
            no check
        2)
        get collection metadata:
            get from system
        3)
        create repository
        4)
        get by id
        '''
        pass

    @abc.abstractmethod
    def create(self, collection_name: str, document: dict):
        '''
        1)
        domain:
            if collectioName not found system:
                raise NotFoundException
        system:
            no check
        2)
        get collection metadata:
            get from system
        3)
        validate
        4)
        create repository
        5)
        key generator
        6) 
        if exists => error
        7)
        create
        '''
        pass

    @abc.abstractmethod
    def update(self, collection_name: str, id: str, document: dict):
        '''
        1)
        domain:
            if collectioName not found system:
                raise NotFoundException
        system:
            no check
        2)
        get collection metadata:
            get from system
        3)
        validate
        4)
        create repository
        5)
        update
        '''
        pass

    @abc.abstractmethod
    def delete(self, collection_name: str, id: str):
        '''
        1)
        domain:
            if collectioName not found system:
                raise NotFoundException
        system:
            no check
        2)
        get collection metadata:
            get from system
        3)
        create repository
        4)
        delete
        '''
        pass

    @abc.abstractmethod
    def query(self, collection_name: str, queryId: str, example: dict):
        '''
        1)
        domain:
            if collectioName not found system:
                raise NotFoundException
        system:
            no check
        2)
        get collection metadata:
            get from system
        3)
        create index service
        4)
        find by example
        '''
        pass

    @staticmethod
    def is_system(collection_name):
        SYSTEM_ENTITIES = os.environ['ENTITIES']
        return collection_name in SYSTEM_ENTITIES.split(",")


class DynamoPlusHandler(DynamoPlusHandlerInterface):
    def __init__(self, *args, **kwargs):
        self.systemService = SystemService()

    def get(self, collection_name: str, id: str):
        is_system = DynamoPlusHandlerInterface.is_system(collection_name)
        if is_system:
            logger.info("Get {} metadata from system".format(collection_name))
            if collection_name == 'collection':
                collection_metadata = self.systemService.get_collection_by_name(id)
                if collection_metadata is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, id))
                logger.info("Found collection {}".format(collection_metadata.__str__))
                return collection_metadata.__dict__
            elif collection_name == 'index':
                index_metadata = self.systemService.get_index(id,collection_name)
                if index_metadata is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, id))
                logger.info("Found index {}".format(index_metadata.__str__))
                return index_metadata.__dict__
        else:
            logger.info("Get {} document".format(collection_name))
            collection_metadata = self.systemService.get_collection_by_name(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            document = DomainService(collection_metadata).get_document(id)
            if document is None:
                raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                       "{} not found with id {}".format(collection_name, id))
            return document

    def create(self, collection_name: str, document: dict) -> dict:
        is_system = DynamoPlusHandlerInterface.is_system(collection_name)
        if is_system:
            logger.info("Creating {} metadata {}".format(collection_name, document))
            if collection_name == 'collection':
                collection_metadata = from_dict_to_collection(document)
                collection_metadata = self.systemService.create_collection(collection_metadata)
                logger.info("Created collection {}".format(collection_metadata.__str__))
                return from_collection_to_dict(collection_metadata)
            elif collection_name == 'index':
                index_metadata = from_dict_to_index(document)
                index_metadata = self.systemService.create_index(index_metadata)
                logger.info("Created index {}".format(index_metadata.__str__))
                return from_index_to_dict(index_metadata)
        else:
            logger.info("Create {} document {}".format(collection_name, document))
            collection_metadata = self.systemService.get_collection_by_name(collection_name)
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

    def update(self, collection_name: str, document: dict):
        """
        1)
        domain:
            if collectioName not found system:
                raise NotFoundException
        system:
            no check
        2)
        get collection metadata:
            get from system
        3)
        validate
        4)
        create repository
        5)
        update
        """
        is_system = DynamoPlusHandlerInterface.is_system(collection_name)
        if is_system:
            raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,"updating {} is not supported ".format(collection_name))
        else:
            logger.info("update {} document {}".format(collection_name, document))
            collection_metadata = self.systemService.get_collection_by_name(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            timestamp = datetime.utcnow()
            document["update_date_time"] = timestamp.isoformat()
            d = DomainService(collection_metadata).update_document(document)
            logger.info("updated document {}".format(d))
            return d

    def delete(self, collection_name: str, id: str):
        is_system = DynamoPlusHandlerInterface.is_system(collection_name)
        if is_system:
            logger.info("delete {} metadata from system".format(collection_name))
            if collection_name == 'collection':
                self.systemService.delete_collection(id)
            elif collection_name == 'index':
                index_metadata = self.systemService.delete_index(id)
        else:
            logger.info("delete {} document {}".format(collection_name,id))
            collection_metadata = self.systemService.get_collection_by_name(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            DomainService(collection_metadata).delete_document(id)

    def query(self, collection_name: str, query_id: str = None, example: dict = None):
        is_system = DynamoPlusHandlerInterface.is_system(collection_name)
        if is_system:
            documents = []
            last_evaluated_key = None
            if collection_name == 'collection':
                collections = self.systemService.get_all_collections()
                return list(map(lambda c: c.__dict__,collections)), None
            elif collection_name == 'index':
                    index_metadata_list = self.systemService.find_indexes_from_collection_name(example["collection"]["name"])
                    documents = list(map(lambda i: i.__dict__, index_metadata_list))
                    return documents, None
            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                   "{} is not a valid collection".format(collection_name))
        else:
            logger.info("query id {} collection {} by example ".format( query_id,collection_name,example))
            collection_metadata = self.systemService.get_collection_by_name(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            domain_service = DomainService(collection_metadata)
            documents = []
            last_evaluated_key = None
            if query_id is None:
                logger.info("Query all {}".format(collection_name))
                documents,last_evaluated_key = domain_service.find_all()
            else:
                index_metadata = self.systemService.get_index(query_id,collection_name)
                if index_metadata is None:
                    raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST, "no index {} found".format(query_id))
                documents,last_evaluated_key = domain_service.find_by_index(index_metadata, example)
            return documents,last_evaluated_key

