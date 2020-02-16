import logging

from typing import Callable, Collection
from dynamoplus.service.dynamoplus import DynamoPlusService
from dynamoplus.repository.models import IndexModel
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.service.system.system import SystemService

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def __indexing(repository_action: Callable[[DynamoPlusRepository], None], system_service: SystemService, collection_name: str,
               collection_metadata: Collection, new_record: dict):

    is_system = DynamoPlusService.is_system(collection_name)
    if not is_system:
        indexes_by_collection_name,last_evaluated_key = system_service.find_indexes_from_collection_name(collection_name)
        for index in indexes_by_collection_name:
            repository = IndexDynamoPlusRepository(collection_metadata,index,False)
            index_model = IndexModel(collection_metadata, new_record,index)
            if index_model.data():
                repository_action(repository)


def create_indexes(collection_name:str, new_record: dict):
    system_service = SystemService()
    collection_metadata = system_service.get_collection_by_name(collection_name)
    if collection_metadata:
        __indexing(lambda r: r.create(new_record), system_service, collection_name, collection_metadata, new_record)
    else:
        logger.debug('Skipping creating indexes on record of type {}:  collection not found'.format(collection_name))

def update_indexes(collection_name:str, new_record: dict):
    system_service = SystemService()
    collection_metadata = system_service.get_collection_by_name(collection_name)
    if collection_metadata:
        __indexing(lambda r: r.update(new_record), system_service, collection_name, collection_metadata, new_record)
    else:
        logger.debug('Skipping creating indexes on record of type {}:  collection not found'.format(collection_name))

def delete_indexes(collection_name:str, new_record: dict):
    system_service = SystemService()
    collection_metadata = system_service.get_collection_by_name(collection_name)
    if collection_metadata:
        id = new_record[collection_metadata.id_key]
        __indexing(lambda r: r.delete(id), system_service, collection_name, collection_metadata, new_record)
    else:
        logger.debug('Skipping deleting indexes on record of type {}:  collection not found'.format(collection_name))
