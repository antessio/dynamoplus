import logging

from typing import Callable
from dynamoplus.v2.repository.repositories import Repository, Model, get_table_name
from dynamoplus.v2.service.system.system_service import Collection
from dynamoplus.v2.service.system.system_service import IndexService,CollectionService
from dynamoplus.v2.service.model_service import get_index_model
from dynamoplus.v2.service.common import is_system



logger = logging.getLogger()
logger.setLevel(logging.INFO)


def __indexing(model_action: Callable[[Model], None],
               collection: Collection,
               new_record: dict):
    is_system_collection = is_system(collection)
    if not is_system_collection:
        logger.debug("{} is not system".format(collection.name))
        indexes_by_collection_name = IndexService.get_indexes_from_collection_name_generator(
            collection.name)
        for index in indexes_by_collection_name:
            logger.debug("found index {}".format(str(index)))

            index_model = get_index_model(collection,index,new_record)
            if index_model.data:
                model_action(index_model)


def create_indexes(collection_name: str, new_record: dict):
    collection_metadata = CollectionService.get_collection(collection_name)

    if collection_metadata:
        __indexing(lambda model: Repository(get_table_name(is_system(collection_metadata))).create(model), collection_metadata, new_record)
    else:
        logger.debug('Skipping creating indexes on record of type {}:  collection not found'.format(collection_name))


def update_indexes(collection_name: str, new_record: dict):
    collection_metadata = CollectionService.get_collection(collection_name)
    if collection_metadata:
        __indexing(lambda model: Repository(get_table_name(is_system(collection_metadata))).update(model), collection_metadata, new_record)
    else:
        logger.debug('Skipping creating indexes on record of type {}:  collection not found'.format(collection_name))


def delete_indexes(collection_name: str, new_record: dict):
    collection_metadata = CollectionService.get_collection(collection_name)
    if collection_metadata:
        id = new_record[collection_metadata.id_key]
        __indexing(lambda model: Repository(get_table_name(is_system(collection_metadata))).delete(model.pk,model.sk), collection_metadata, new_record)
    else:
        logger.debug('Skipping deleting indexes on record of type {}:  collection not found'.format(collection_name))
