import logging
from typing import *

from dynamoplus.models.query.query import Query
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.repository.models import Model, QueryResult
from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType
from dynamoplus.models.system.index.index import Index
from dynamoplus.models.system.client_authorization.client_authorization import ClientAuthorization, \
    ClientAuthorizationApiKey, ClientAuthorizationHttpSignature, Scope, ScopesType

collectionMetadata = Collection("collection", "name")
indexMetadata = Collection("index", "uid")
client_authorization_metadata = Collection("client_authorization", "client_id")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def from_collection_to_dict(collection_metadata: Collection):
    result = {"name": collection_metadata.name, "id_key": collection_metadata.id_key}
    if collection_metadata.ordering_key:
        result["ordering_key"] = collection_metadata.ordering_key
    return result


def from_index_to_dict(index_metadata: Index):
    return {"name": index_metadata.index_name, "collection": {"name": index_metadata.collection_name},
            "conditions": index_metadata.conditions}


def from_dict_to_index(d: dict):
    return Index(d["uid"], d["collection"]["name"], d["conditions"], d["ordering_key"] if "ordering_key" in d else None,
                 d["index_name"] if "index_name" in d else None)


# def from_dict_to_client_authorization(d: dict):
#     client_id = d["client_id"]
#     scopes = list(map(lambda s: Scope(s["collection_name"], ScopesType(s["scope_type"])), d["scopes"]))
#     if d["type"].lower() == 'api-key':
#         api_key = d["api_key"]
#         whitelist_hosts = d["whitelist_hosts"] if "whitelist_hosts" in d else None
#         return ClientAuthorizationApiKey(client_id, scopes, api_key, whitelist_hosts)
#     elif d["type"] == 'http-signature':
#         return ClientAuthorizationHttpSignature(client_id, scopes, d["public_key"])
#     else:
#         raise NotImplementedError

def from_client_authorization_http_signature_to_dict(client_authorization: ClientAuthorizationHttpSignature):
    return {
        "type": "api-key",
        "client_id": client_authorization.client_id,
        "scopes": list( map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type},client_authorization.client_scopes)),
        "public_key": client_authorization.client_public_key
    }

def from_client_authorization_api_key_to_dict(client_authorization: ClientAuthorizationApiKey):
    result = {
        "type": "api-key",
        "client_id": client_authorization.client_id,
        "scopes": list(map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type},
                           client_authorization.client_scopes)),
        "api_key": client_authorization.api_key,
    }
#     if client_authorization.whitelist_hosts:
#         result["whitelist_hosts"] = client_authorization.whitelist_hosts
#     return result


def from_index_to_dict(index_metadata: Index):
    return {
        "uid": index_metadata.uid,
        "name": index_metadata.index_name,
        "collection": {
            "name": index_metadata.collection_name
        },
        "ordering_key": index_metadata._ordering_key,
        "conditions": index_metadata.conditions

    }


def from_collection_to_dict(collection: Collection):
    d = {
        "name": collection.name,
        "id_key": collection.id_key,
        "ordering": collection.ordering_key
        ## TODO attributes definition
    }
    # if collection.attributeDefinition:

    return d


def from_dict_to_collection(d: dict):
    return Collection(d["name"], d["id_key"], d["ordering"] if "ordering" in d else None)


def from_dict_to_client_authorization_http_signature(d: dict):
    client_scopes = list(map(lambda c: Scope(c["collection_name"], ScopesType[c["scope"]]), d["client_scopes"]))
    return ClientAuthorizationHttpSignature(d["client_id"], client_scopes, d["public_key"])


def from_dict_to_client_authorization_api_key(d: dict):
    client_scopes = list(map(lambda c: Scope(c["collection_name"], ScopesType[c["scope"]]), d["client_scopes"]))
    return ClientAuthorizationApiKey(d["client_id"], client_scopes, d["api_key"], d["whitelist_hosts"])


from_dict_to_client_authorization_factory = {
    "api_key": from_dict_to_client_authorization_api_key,
    "http_signature": from_dict_to_client_authorization_http_signature
}


def from_dict_to_client_authorization(d: dict):
    return from_dict_to_client_authorization_factory[d["type"]](d)


class SystemService:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_client_authorization(client_id: str):
        repository = DynamoPlusRepository(client_authorization_metadata, True)
        model = repository.get(client_id)
        if model:
            return from_dict_to_client_authorization(model.document)

    @staticmethod
    def create_collection(metadata: Collection):
        collection = from_collection_to_dict(metadata)
        repository = DynamoPlusRepository(collectionMetadata, True)
        model = repository.create(collection)
        return from_dict_to_collection(model.document)

    # def updateCollection(self, metadata:Collection):
    #     collection=self.fromCollectionToDict(metadata)
    #     repository = DynamoPlusRepository(collectionMetadata)
    #     model = repository.update(collection)
    #     return self.fromDictToCollection(model.document)
    @staticmethod
    def delete_collection(name: str):
        DynamoPlusRepository(collectionMetadata, True).delete(name)

    @staticmethod
    def get_all_collections(limit: int = None, start_from: str = None):
        index_metadata = Index(None, "collection", [])
        query = Query({}, index_metadata, limit, start_from)
        result = IndexDynamoPlusRepository(collectionMetadata, True, index_metadata).find(query)
        if result:
            return list(map(lambda m: from_dict_to_collection(m.document), result.data)), result.lastEvaluatedKey

    @staticmethod
    def get_collection_by_name(name: str):
        model = DynamoPlusRepository(collectionMetadata, True).get(name)
        if model:
            return from_dict_to_collection(model.document)

    @staticmethod
    def create_index(i: Index) -> Index:
        index = from_index_to_dict(i)
        repository = DynamoPlusRepository(indexMetadata, True)
        model = repository.create(index)
        if model:
            created_index = from_dict_to_index(model.document)
            logger.info("index created {}".format(created_index.__str__()))
            index_by_collection_name = IndexDynamoPlusRepository(indexMetadata,
                                                                 Index(None, "index", ["collection.name"]),
                                                                 True).create(model.document)
            logger.info(
                "{} has been indexed {}".format(created_index.collection_name, index_by_collection_name.document))
            index_by_name = IndexDynamoPlusRepository(indexMetadata, Index(None, "index", ["collection.name", "name"]),
                                                      True).create(model.document)
            logger.info("{} has been indexed {}".format(created_index.collection_name, index_by_name.document))
            return created_index

    @staticmethod
    def get_index(name: str, collection_name: str):
        # model = DynamoPlusRepository(indexMetadata, True).get(name)
        # if model:
        #     return from_dict_to_index(model.document)
        index = Index(None, "index", ["collection.name", "name"])
        query = Query({"name": name, "collection": {"name": collection_name}}, index)
        result: QueryResult = IndexDynamoPlusRepository(indexMetadata, index, True).find(query)
        indexes = list(map(lambda m: from_dict_to_index(m.document), result.data))
        if len(indexes) == 0:
            return None
        else:
            return indexes[0]

    @staticmethod
    def delete_index(name: str):
        DynamoPlusRepository(indexMetadata, True).delete(name)

    @staticmethod
    def find_indexes_from_collection_name(collection_name: str, limit: int = None, start_from: str = None):
        index = Index(None, "index", ["collection.name"])
        query = Query({"collection": {"name": collection_name}}, index, limit, start_from)
        result: QueryResult = IndexDynamoPlusRepository(indexMetadata, index, True).find(query)
        return list(map(lambda m: from_dict_to_index(m.document), result.data)), result.lastEvaluatedKey

    @staticmethod
    def find_collections_by_example(example: Collection):
        index = Index(None, "collection", ["name"])
        query = Query({"name": example.name}, index)
        result: QueryResult = IndexDynamoPlusRepository(indexMetadata, index, True).find(query)
        return list(map(lambda m: from_dict_to_collection(m.document), result.data))
