import logging

from dynamoplus.models.system.aggregation.aggregation import AggregationTrigger
# from dynamoplus.models.system.aggregation.aggregation import AggregationTrigger
from dynamoplus.models.system.index.index import IndexConfiguration
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.v2.repository.domain_repository import IndexDomainEntity
from dynamoplus.v2.repository.repositories_v2 import IndexingOperation, IndexModel
from dynamoplus.v2.service.domain.domain_service_v2 import DomainService
from dynamoplus.v2.service.system.aggregation_service_v2 import AggregationProcessingService
from dynamoplus.v2.service.system.system_service_v2 import IndexService, CollectionService, AggregationService, Index
from dynamoplus.v2.service.common import is_system
from dynamoplus.utils.utils import find_added_values, find_removed_values, find_updated_values, \
    filter_out_not_included_fields

from dynamoplus.v2.service.system.system_service_v2 import AggregationConfigurationService

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class IndexingService:

    def __init__(self, index_service: IndexService,
                 collection_service: CollectionService,
                 domain_service: DomainService,
                 aggregation_configuration_service: AggregationConfigurationService,
                 aggregation_service: AggregationService):
        self.collection_service = collection_service
        self.index_service = index_service
        self.domain_service = domain_service
        self.aggregation_configuration_service = aggregation_configuration_service
        self.aggregation_service = aggregation_service

    def create_indexes(self, collection_name: str, new_record: dict):
        collection = self.collection_service.get_collection(collection_name)

        if collection:
            self.indexing(collection, new_record, None)
        else:
            logger.debug(
                'Skipping creating indexes on record of type {}:  collection not found'.format(collection_name))

    def indexing(self, collection_metadata: Collection,
                 new_record: dict,
                 old_record: dict):
        is_system_collection = is_system(collection_metadata)
        if not is_system_collection:
            ## This doesn't work, if an attribute not indexed it's updated then it doesn't update it
            to_remove_index_models = get_index_models_to_remove(self.index_service, collection_metadata, new_record,
                                                                old_record)
            to_add_index_models = get_index_models_to_add(self.index_service, collection_metadata, new_record,
                                                          old_record)
            to_update_index_models = get_index_models_to_update(self.index_service, collection_metadata, new_record,
                                                                old_record)

            self.domain_service.indexing(
                IndexingOperation(to_remove_index_models, to_update_index_models, to_add_index_models))

            aggregations = self.aggregation_configuration_service.get_aggregation_configurations_by_collection_name_generator(
                collection_metadata.name)
            trigger = AggregationTrigger.UPDATE
            if old_record is None:
                trigger = AggregationTrigger.INSERT
            elif new_record is None:
                trigger = AggregationTrigger.DELETE
            for a in aggregations:
                if trigger in a.on:
                    # self.aggregation_configuration_service.execute_aggregation(a, collection_metadata, new_record, old_record)
                    AggregationProcessingService(a, self.aggregation_service,
                                                 self.aggregation_configuration_service).execute_aggregation(
                        collection_metadata, new_record, old_record)


def get_index_models_to_remove(index_service: IndexService, collection_metadata, new_record: dict, old_record: dict):
    to_remove = []
    if old_record is not None and len(old_record.keys()) > 0:
        removed = find_removed_values(old_record, new_record)
        # changed_fields = get_all_keys(removed)
        to_remove = find_matching_indexes(index_service, removed, collection_metadata, old_record) if removed else []
    return to_remove


def get_index_models_to_add(index_service: IndexService, collection_metadata, new_record, old_record):
    to_add = []
    if new_record is not None:
        added = find_added_values(old_record, new_record)
        to_add = find_matching_indexes(index_service, added, collection_metadata, new_record) if added else []
    return to_add


def get_index_models_to_update(index_service: IndexService, collection_metadata, new_record, old_record):
    to_update = []
    if old_record is not None and new_record is not None:
        logger.debug("updated index new record = {} and old record = {}".format(new_record, old_record))
        updated = find_updated_values(old_record, new_record)
        to_update = find_matching_indexes(index_service, updated, collection_metadata, new_record) if updated else []
    return to_update


def find_matching_indexes(index_service: IndexService, values: dict,
                          collection_metadata: Collection,
                          record: dict):
    result = []
    if values:
        logger.debug("changed dict = {} while new record is {} ".format(values, record))
        changed_fields = get_all_keys(values)
        logger.debug("changed fields = {}".format(changed_fields))
        for index in index_service.get_indexes_from_collection_name_generator(collection_metadata.name):
            for field in changed_fields:
                if field in index.conditions or index.index_configuration == IndexConfiguration.OPTIMIZE_READ or field == index.ordering_key:
                    document = record if index and (
                            index.index_configuration is None or index.index_configuration == IndexConfiguration.OPTIMIZE_READ) \
                        else filter_out_not_included_fields(record, index.conditions + [collection_metadata.id_key])

                    index_model = get_index_model(collection_metadata, index, document)
                    if index_model not in result:
                        result.append(index_model)
    return result


def extract_value(data_dict, keys):
    if not keys:
        return data_dict

    key = keys[0]
    rest_of_keys = keys[1:]

    if key in data_dict:
        if rest_of_keys:
            # If there are more keys, continue navigating the nested structure
            return extract_value(data_dict[key], rest_of_keys)
        else:
            # If this is the last key, return the value
            return data_dict[key]
    else:
        return None  # Key not found


def get_index_model(collection_metadata: Collection, index: Index, document: dict):
    index_values = []
    for attr in index.conditions:
        value = extract_value(document, attr.split('.'))
        if value:
            index_values.append(value)

    index_value = '#'.join(index_values)
    index_name = collection_metadata.name + "#" + "__".join(index.conditions)
    return IndexDomainEntity(collection_metadata.name,
                             document[collection_metadata.id_key],
                             index_name,
                             index_value,
                             document if index.index_configuration == IndexConfiguration.OPTIMIZE_READ else None)


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
