import logging

from typing import Callable

from dynamoplus.models.system.index.index import IndexConfiguration
from dynamoplus.service.dynamoplus import DynamoPlusService
from dynamoplus.repository.models import IndexModel, Collection
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.service.system.system import SystemService
from dynamoplus.utils.utils import find_added_values, find_removed_values, find_updated_values, \
    filter_out_not_included_fields

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def __indexing(system_service: SystemService,
               collection_name: str,
               collection_metadata: Collection,
               new_record: dict,
               old_record: dict):
    is_system = DynamoPlusService.is_system(collection_name)
    if not is_system:
        to_remove_index_models = get_index_models_to_remove(collection_metadata, collection_name, new_record,
                                                            old_record, system_service)
        to_add_index_models = get_index_models_to_add(collection_metadata, collection_name, new_record,
                                                      old_record, system_service)
        to_update_index_models = get_index_models_to_update(collection_metadata, collection_name, new_record,
                                                            old_record, system_service)

        for remove in to_remove_index_models:
            repository = IndexDynamoPlusRepository(collection_metadata, remove.index, False)
            repository.delete(remove.document[remove.id_key])

        for add in to_add_index_models:
            repository = IndexDynamoPlusRepository(collection_metadata, add.index, False)
            repository.create(add.document)

        for update in to_update_index_models:
            repository = IndexDynamoPlusRepository(collection_metadata, update.index, False)
            repository.update(update.document)


def get_index_models_to_remove(collection_metadata, collection_name, new_record: dict, old_record: dict,
                               system_service):
    to_remove = []
    if old_record is not None and len(old_record.keys()) > 0:
        removed = find_removed_values(old_record, new_record)
        # changed_fields = get_all_keys(removed)
        to_remove = find_matching_indexes(removed, collection_metadata, collection_name, old_record,
                                          system_service) if removed else []
    return to_remove


def get_index_models_to_add(collection_metadata, collection_name, new_record, old_record,
                            system_service):
    added = find_added_values(old_record, new_record)
    return find_matching_indexes(added, collection_metadata, collection_name, new_record,
                                 system_service) if added else []


def get_index_models_to_update(collection_metadata, collection_name, new_record, old_record,
                               system_service):
    updated = find_updated_values(old_record, new_record)
    return find_matching_indexes(updated, collection_metadata, collection_name, new_record,
                                 system_service) if updated else []


def find_matching_indexes(values: dict,
                          collection_metadata: Collection,
                          collection_name: str,
                          record: dict,
                          system_service: SystemService):
    result = []
    if values:
        changed_fields = get_all_keys(values)
        logger.debug("changed fields = {}".format(changed_fields))
        for index in system_service.get_indexes_from_collection_name_generator(collection_name):
            for field in changed_fields:
                if field in index.conditions:
                    document = record if index and index.index_configuration == IndexConfiguration.OPTIMIZE_READ \
                        else filter_out_not_included_fields(record, index.conditions + [collection_metadata.id_key])
                    result.append(IndexModel(collection_metadata, document, index))
    return result


## TODO: add test and move to utils
def get_all_keys(d):
    keys = []
    for key, value in d.items():
        if type(value) is dict:
            for nested_key in get_all_keys(value):
                keys.append(key + "." + nested_key)
        else:
            keys.append(key)
    return keys


def create_indexes(collection_name: str, new_record: dict):
    system_service = SystemService()
    collection_metadata = system_service.get_collection_by_name(collection_name)
    if collection_metadata:
        __indexing(system_service, collection_name, collection_metadata, new_record, {})
    else:
        logger.debug('Skipping creating indexes on record of type {}:  collection not found'.format(collection_name))


def update_indexes(collection_name: str, new_record: dict, old_record: dict):
    system_service = SystemService()
    collection_metadata = system_service.get_collection_by_name(collection_name)
    if collection_metadata:
        __indexing(system_service, collection_name, collection_metadata, new_record, old_record)
    else:
        logger.debug('Skipping creating indexes on record of type {}:  collection not found'.format(collection_name))


def delete_indexes(collection_name: str, old_record: dict):
    system_service = SystemService()
    collection_metadata = system_service.get_collection_by_name(collection_name)
    if collection_metadata:
        __indexing(system_service, collection_name, collection_metadata, {}, old_record)
    else:
        logger.debug('Skipping deleting indexes on record of type {}:  collection not found'.format(collection_name))
