import logging

#from dynamoplus.models.system.aggregation.aggregation import AggregationTrigger
from dynamoplus.models.system.aggregation.aggregation import AggregationTrigger
from dynamoplus.models.system.index.index import IndexConfiguration
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.v2.service.system.system_service import IndexService, CollectionService, AggregationConfigurationService
from dynamoplus.v2.service.model_service import get_index_model
from dynamoplus.v2.service.common import is_system, get_repository_factory
from dynamoplus.utils.utils import find_added_values, find_removed_values, find_updated_values, \
    filter_out_not_included_fields

from dynamoplus.v2.service.system.aggregation_service import AggregationProcessingService

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def __indexing(collection_metadata: Collection,
               new_record: dict,
               old_record: dict):
    is_system_collection = is_system(collection_metadata)
    if not is_system_collection:
        ## This doesn't work, if an attribute not indexed it's updated then it doesn't update it
        to_remove_index_models = get_index_models_to_remove(collection_metadata, new_record, old_record)
        to_add_index_models = get_index_models_to_add(collection_metadata, new_record, old_record)
        to_update_index_models = get_index_models_to_update(collection_metadata, new_record, old_record)

        repository = get_repository_factory(collection_metadata)
        for remove in to_remove_index_models:
            repository.delete(remove.pk, remove.sk)

        for add in to_add_index_models:
            repository.create(add)

        for update in to_update_index_models:
            repository.update(update)

        aggregations = AggregationConfigurationService.get_aggregation_configurations_by_collection_name(collection_metadata.name)
        trigger = AggregationTrigger.UPDATE
        if old_record is None:
            trigger = AggregationTrigger.INSERT
        elif new_record is None:
            trigger = AggregationTrigger.DELETE
        for a in aggregations:
            if trigger in a.on:
                AggregationProcessingService.execute_aggregation(a, collection_metadata,new_record,old_record)


def get_index_models_to_remove(collection_metadata, new_record: dict, old_record: dict):
    to_remove = []
    if old_record is not None and len(old_record.keys()) > 0:
        removed = find_removed_values(old_record, new_record)
        # changed_fields = get_all_keys(removed)
        to_remove = find_matching_indexes(removed, collection_metadata, old_record) if removed else []
    return to_remove


def get_index_models_to_add(collection_metadata, new_record, old_record):
    to_add = []
    if new_record is not None:
        added = find_added_values(old_record, new_record)
        to_add = find_matching_indexes(added, collection_metadata, new_record) if added else []
    return to_add


def get_index_models_to_update(collection_metadata, new_record, old_record):
    to_update = []
    if old_record is not None and new_record is not None:
        updated = find_updated_values(old_record, new_record)
        to_update = find_matching_indexes(updated, collection_metadata, new_record) if updated else []
    return to_update


def find_matching_indexes(values: dict,
                          collection_metadata: Collection,
                          record: dict):
    result = []
    if values:
        changed_fields = get_all_keys(values)
        logger.debug("changed fields = {}".format(changed_fields))
        for index in IndexService.get_indexes_from_collection_name_generator(collection_metadata.name):
            for field in changed_fields:
                if field in index.conditions:
                    document = record if index and (
                                index.index_configuration is None or index.index_configuration == IndexConfiguration.OPTIMIZE_READ) \
                        else filter_out_not_included_fields(record, index.conditions + [collection_metadata.id_key])
                    index_model = get_index_model(collection_metadata, index, document)
                    if not index_model in result:
                        result.append(index_model)
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
    collection_metadata = CollectionService.get_collection(collection_name)

    if collection_metadata:
        __indexing(collection_metadata, new_record, None)
    else:
        logger.debug('Skipping creating indexes on record of type {}:  collection not found'.format(collection_name))


def update_indexes(collection_name: str, old_record: dict, new_record: dict):
    collection_metadata = CollectionService.get_collection(collection_name)
    if collection_metadata:
        __indexing(collection_metadata, new_record, old_record)
    else:
        logger.debug('Skipping creating indexes on record of type {}:  collection not found'.format(collection_name))


def delete_indexes(collection_name: str, new_record: dict):
    collection_metadata = CollectionService.get_collection(collection_name)

    if collection_metadata:
        __indexing(collection_metadata, None, new_record)
    else:
        logger.debug('Skipping deleting indexes on record of type {}:  collection not found'.format(collection_name))
