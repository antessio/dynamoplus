import logging

from dynamoplus.service.indexing_service import create_indexes,update_indexes,delete_indexes
import os

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
            domain_service = args[0]
            collection = domain_service.collection
            logger.info("create document index for {}".format(collection.name))
            create_indexes(collection.name,result)
        return result

    return create


def update_document(fun):
    def update(*args,**kwargs):
        is_local_env = is_local_environment()
        result = fun(*args,**kwargs)
        if result and is_local_env:
            domain_service = args[0]
            collection = domain_service.collection
            update_indexes(collection.name,result)
            logger.info("updating document index for {}".format(collection.name))
        return result

    return update

def delete_document(fun):
    def delete(*args,**kwargs):
        is_local_env = is_local_environment()
        result = fun(*args,**kwargs)
        if result and is_local_env:
            domain_service = args[0]
            collection = domain_service.collection
            delete_indexes(collection.name,result)
            logger.info("delete document index for {}".format(collection.name))
        return result

    return delete


