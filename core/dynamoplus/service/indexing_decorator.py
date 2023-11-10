import logging
import os
import uuid

# from dynamoplus.service.indexing_service import create_indexes,update_indexes,delete_indexes
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.v2.indexing_service_v2 import IndexingService
from dynamoplus.v2.service.common import is_system_collection as is_system

logger = logging.getLogger()
logger.setLevel(logging.INFO)
local = False


def is_local_environment():
    return "STAGE" in os.environ and "local" == os.environ["STAGE"] and (
            "TEST_FLAG" not in os.environ or "true" != os.environ["TEST_FLAG"])


def create_document(fun):
    def create(instance, *args, **kwargs):
        is_local_env = is_local_environment()
        if is_local_env :
            result = fun(instance,*args, **kwargs)
            if result and is_local_env:
                collection_name = args[0]
                is_system_collection = is_system(collection_name)
                if not is_system_collection:
                    if result and isinstance(result, dict):
                        logger.info("create document index for {}".format(collection_name))
                        IndexingService(instance.index_service,
                                        instance.collection_service,
                                        instance.domain_service,
                                        instance.aggregation_configuration_service,
                                        instance.aggregation_service).create_indexes(collection_name,
                                                                                                   result)
            return result

    return create


def update_document(fun):
    def update(instance, *args, **kwargs):
        is_local_env = is_local_environment()
        if is_local_env :
            collection_name = args[0]
            id = args[2]
            is_system_collection = is_system(collection_name)
            before = None
            collection = None
            if is_system_collection:
                if collection_name == 'index':
                    before = instance.index_service.get_index_by_id(uuid.UUID(id)).to_dict()
                    collection = Collection("index", "id")
                elif collection_name == 'collection':
                    before = instance.collection_service.get_collection(collection_name).to_dict()
                    collection = Collection("collection", "name")
                elif collection_name == 'client_authorization':
                    before = instance.client_authorization_service.get_client_authorization(uuid.UUID(id)).to_dict()
                    collection = Collection("client_authorization", "id")

            else:
                collection = instance.collection_service.get_collection(collection_name)
                before = instance.domain_service.get_document(id, collection)
            after = fun(instance, *args, **kwargs)
            if after and is_local_env:
                if not is_system_collection:
                    IndexingService(instance.index_service,
                                    instance.collection_service,
                                    instance.domain_service,
                                    instance.aggregation_configuration_service,
                                        instance.aggregation_service).indexing(collection, after, before)
                    logger.info("updating document index for {}".format(collection_name))
            return after

    return update


def delete_document(fun):
    def delete(instance, *args, **kwargs):
        is_local_env = is_local_environment()
        if is_local_env :
            collection = None
            before = None
            collection_name = args[0]
            id = args[1]
            is_system_collection = is_system(collection_name)
            if not is_system_collection:
                collection = instance.collection_service.get_collection(collection_name)
                before = instance.domain_service.get_document(id, collection)
            fun(instance, *args, **kwargs)
            if before:
                IndexingService(instance.index_service,
                                instance.collection_service,
                                instance.domain_service,
                                instance.aggregation_configuration_service,
                                        instance.aggregation_service).indexing(collection, None, before)
            logger.info("delete document index for {}".format(collection_name))
        else:
            fun(instance, *args, **kwargs)

    return delete
