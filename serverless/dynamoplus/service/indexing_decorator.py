import logging
import uuid

#from dynamoplus.service.indexing_service import create_indexes,update_indexes,delete_indexes
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.v2.indexing_service_v2 import create_indexes,update_indexes,delete_indexes
from dynamoplus.v2.service.domain.domain_service import DomainService
from dynamoplus.v2.service.system.system_service import CollectionService
import os

from dynamoplus.v2.service.common import is_system
from dynamoplus.v2.service.system.system_service_v2 import IndexService
from dynamoplus.v2.service.system.system_service_v2 import CollectionService as CollectionServiceV2
from dynamoplus.v2.service.system.system_service_v2 import AuthorizationService

logger = logging.getLogger()
logger.setLevel(logging.INFO)
local = False


def is_local_environment():
    return "STAGE" in os.environ and "local" == os.environ["STAGE"] and (
                "TEST_FLAG" not in os.environ or "true" != os.environ["TEST_FLAG"])


def create_document(fun):
    def create(*args,**kwargs):
        is_local_env = is_local_environment()
        result = fun(*args,**kwargs)
        if result and is_local_env:
            collection_name = args[1]
            is_system_collection = is_system(Collection(collection_name, None))
            if not is_system_collection:
                if result and isinstance(result,dict):
                    logger.info("create document index for {}".format(collection_name))
                    create_indexes(collection_name,result)
        return result

    return create


def update_document(fun):
    def update(*args,**kwargs):
        is_local_env = is_local_environment()
        collection_name = args[1]
        id = args[3]
        is_system_collection = is_system(Collection(collection_name, None))
        before = None
        if is_system_collection:
            if collection_name == 'index':
                before = IndexService().get_index_by_id(uuid.UUID(id)).to_dict()
            elif collection_name == 'collection':
                before = CollectionServiceV2().get_collection(collection_name).to_dict()
            elif collection_name == 'client_authorization':
                before = AuthorizationService().get_client_authorization(uuid.UUID(id)).to_dict()

        else:
            before = DomainService(CollectionService.get_collection(collection_name)).get_document(id)
        after = fun(*args,**kwargs)
        if after and is_local_env:
            if not is_system_collection:
                update_indexes(collection_name,before,after)
                logger.info("updating document index for {}".format(collection_name))
        return after

    return update

def delete_document(fun):
    def delete(*args,**kwargs):
        is_local_env = is_local_environment()
        if is_local_env:
            collection_name = args[1]
            id = args[2]
            is_system_collection = is_system(Collection(collection_name, None))
            if not is_system_collection:
                before = DomainService(CollectionService.get_collection(collection_name)).get_document(id)
            fun(*args, **kwargs)
            if before:
                delete_indexes(collection_name,before)
            logger.info("delete document index for {}".format(collection_name))
        else:
            fun(*args, **kwargs)


    return delete


