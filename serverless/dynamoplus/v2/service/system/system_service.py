import logging
from decimal import Decimal
from typing import *

from dynamoplus.models.query.conditions import Eq, And, AnyMatch, Predicate
from dynamoplus.models.system.aggregation.aggregation import Aggregation, AggregationTrigger, AggregationJoin, \
    AggregationType
from dynamoplus.models.system.client_authorization.client_authorization import ClientAuthorization, \
    ClientAuthorizationApiKey, ClientAuthorizationHttpSignature, Scope, ScopesType
from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType, \
    AttributeConstraint
from dynamoplus.models.system.index.index import Index, IndexConfiguration
from dynamoplus.v2.service.common import get_repository_factory
from dynamoplus.v2.service.model_service import get_model, get_index_model
from dynamoplus.v2.service.query_service import QueryService

collection_metadata = Collection("collection", "name")
index_metadata = Collection("index", "name")
client_authorization_metadata = Collection("client_authorization", "client_id")
aggregation_metadata = Collection("aggregation", "name")

index_by_collection_and_name_metadata = Index(index_metadata.name, ["collection.name", "name"], None)
index_by_collection_metadata = Index(index_metadata.name, ["collection.name"], None)
index_by_name_metadata = Index(index_metadata.name, ["name"], None)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Converter:

    @staticmethod
    def from_collection_to_dict(collection: Collection):
        result = {"name": collection.name, "id_key": collection.id_key}
        if collection.ordering_key:
            result["ordering_key"] = collection.ordering_key
        return result

    @staticmethod
    def from_index_to_dict(index: Index):
        d = {"name": index.index_name,
             "collection": {"name": index.collection_name},
             "conditions": index.conditions}
        if index.ordering_key:
            d["ordering_key"] = index.ordering_key
        if index.index_configuration:
            d["configuration"] = index.index_configuration.name
        return d

    @staticmethod
    def from_dict_to_index(d: dict):
        return Index(d["collection"]["name"], d["conditions"],
                     IndexConfiguration.value_of(d["configuration"]) if "configuration" in d else None,
                     d["ordering_key"] if "ordering_key" in d else None)

    @staticmethod
    def from_client_authorization_http_signature_to_dict(client_authorization: ClientAuthorizationHttpSignature):
        return {
            "type": "http_signature",
            "client_id": client_authorization.client_id,
            "client_scopes": list(map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type.name},
                                      client_authorization.client_scopes)),
            "public_key": client_authorization.client_public_key
        }

    @staticmethod
    def from_client_authorization_api_key_to_dict(client_authorization: ClientAuthorizationApiKey):
        result = {
            "type": "api_key",
            "client_id": client_authorization.client_id,
            "client_scopes": list(map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type.name},
                                      client_authorization.client_scopes)),
            "api_key": client_authorization.api_key,
        }
        if client_authorization.whitelist_hosts:
            result["whitelist_hosts"] = client_authorization.whitelist_hosts
        return result

    @staticmethod
    def from_collection_to_dict(collection: Collection):
        d = {
            "name": collection.name,
            "id_key": collection.id_key,
            "ordering": collection.ordering_key,
            "auto_generate_id": collection.auto_generate_id
        }
        if collection.attribute_definition:
            attributes = list(
                map(lambda a: Converter.from_attribute_definition_to_dict(a), collection.attribute_definition))
            d["attributes"] = attributes
        return d

    @staticmethod
    def from_dict_to_collection(d: dict):
        attributes = list(
            map(Converter.from_dict_to_attribute_definition, d["attributes"])) if "attributes" in d else None
        auto_generate_id = d["auto_generate_id"] if "auto_generate_id" in d else False
        return Collection(d["name"], d["id_key"], d["ordering"] if "ordering" in d else None, attributes,
                          auto_generate_id)

    @staticmethod
    def from_attribute_definition_to_dict(attribute: AttributeDefinition):
        nested_attributes = list(
            map(lambda a: Converter.from_attribute_definition_to_dict(a),
                attribute.attributes)) if attribute.attributes else None
        return {"name": attribute.name, "type": attribute.type.value, "attributes": nested_attributes}

    @staticmethod
    def from_dict_to_attribute_definition(d: dict):
        attributes = None
        if "attributes" in d and d["attributes"] is not None:
            attributes = list(map(Converter.from_dict_to_attribute_definition, d["attributes"]))
        return AttributeDefinition(d["name"], Converter.from_string_to_attribute_type(d["type"]),
                                   Converter.from_array_to_constraints_list(
                                       d["constraints"]) if "constraints" in d else None,
                                   attributes)

    @staticmethod
    def from_array_to_constraints_list(constraints: List[dict]):
        attribute_constraint_map = {
            "NULLABLE": AttributeConstraint.NULLABLE,
            "NOT_NULL": AttributeConstraint.NOT_NULL
        }
        return list(
            map(lambda c: attribute_constraint_map[
                c] if c in attribute_constraint_map else AttributeConstraint.NULLABLE,
                constraints))

    @staticmethod
    def from_string_to_attribute_type(attribute_type: str):
        attribute_types_map = {
            "STRING": AttributeType.STRING,
            "OBJECT": AttributeType.OBJECT,
            "NUMBER": AttributeType.NUMBER,
            "DATE": AttributeType.DATE,
            "ARRAY": AttributeType.ARRAY
        }
        return attribute_types_map[attribute_type] if attribute_type in attribute_types_map else AttributeType.STRING

    @staticmethod
    def from_dict_to_client_authorization_http_signature(d: dict):
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationHttpSignature(d["client_id"], client_scopes, d["public_key"])

    @staticmethod
    def from_dict_to_client_authorization_api_key(d: dict):
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationApiKey(d["client_id"], client_scopes, d["api_key"],
                                         d["whitelist_hosts"] if "whitelist_hosts" in d else None)

    @staticmethod
    def from_dict_to_client_authorization_factory():
        return {
            "api_key": Converter.from_dict_to_client_authorization_api_key,
            "http_signature": Converter.from_dict_to_client_authorization_http_signature
        }

    @staticmethod
    def from_client_authorization_to_dict(client_authorization: ClientAuthorization):
        if isinstance(client_authorization, ClientAuthorizationApiKey):
            return Converter.from_client_authorization_api_key_to_dict(client_authorization)
        elif isinstance(client_authorization, ClientAuthorizationHttpSignature):
            return Converter.from_client_authorization_http_signature_to_dict(client_authorization)
        else:
            raise NotImplementedError("client_authorization not implemented")

    @staticmethod
    def from_dict_to_client_authorization(d: dict):
        return Converter.from_dict_to_client_authorization_factory()[d["type"]](d)

    @staticmethod
    def from_aggregation_to_dict(aggregation: Aggregation):
        a = {
            "on": list(map(lambda o: o.name, aggregation.on))
        }
        d = {
            "collection": {
                "name": aggregation.collection_name
            },
            "type": aggregation.type.name
        }
        if aggregation.join:
            a["join"] = {
                "collection_name": aggregation.join.collection_name,
                "using_field": aggregation.join.using_field
            }
        if aggregation.target_field:
            a["target_field"] = aggregation.target_field
        if aggregation.matches:
            a["matches"] = Converter.from_predicate_to_dict(aggregation.matches)
        d["aggregation"] = a
        d["name"] = aggregation.name
        return d

    @staticmethod
    def from_predicate_to_dict(predicate: Predicate):
        d = {}
        if isinstance(predicate, And):
            d["and"] = list(map(lambda p: Converter.from_predicate_to_dict(p), predicate.conditions))
        elif isinstance(predicate, Eq):
            d["eq"] = {
                "field_name": predicate.field_name,
                "value": predicate.value
            }
        return d

    @staticmethod
    def from_dict_to_predicate(document: dict):
        if "and" in document:
            and_predicate = list(map(lambda p: Converter.from_dict_to_predicate(p), document["and"]))
            return And(and_predicate)
        elif "eq" in document:
            e = document["eq"]
            return Eq(e["field_name"], e["value"])

    @staticmethod
    def from_dict_to_aggregation(document: dict):
        collection_name = document["collection"]["name"]
        t = AggregationType.value_of(document["type"])
        inner_aggregation_document = document["aggregation"]
        on = list(map(lambda o: AggregationTrigger.value_of(o), inner_aggregation_document["on"]))
        if "target_field" in inner_aggregation_document:
            target_field = inner_aggregation_document["target_field"]
        if "join" in inner_aggregation_document:
            join = AggregationJoin(inner_aggregation_document["join"]["collection_name"],
                                   inner_aggregation_document["join"]["using_field"])
        if "matches" in inner_aggregation_document:
            matches = Converter.from_dict_to_predicate(inner_aggregation_document["matches"])

        return Aggregation(collection_name, t, on, target_field, matches, join)


class CollectionService:

    @staticmethod
    def get_collection(collection_name: str) -> Collection:
        repo = get_repository_factory(collection_metadata)
        model = get_model(collection_metadata, {"name": collection_name})
        result = repo.get(model.pk, model.sk)
        if result:
            return Converter.from_dict_to_collection(result.document)

    @staticmethod
    def create_collection(collection: Collection) -> Collection:
        repo = get_repository_factory(collection_metadata)
        model = get_model(collection_metadata, Converter.from_collection_to_dict(collection))
        result = repo.create(model)
        return Converter.from_dict_to_collection(result.document)

    @staticmethod
    def delete_collection(collection_name: str):
        repo = get_repository_factory(collection_metadata)
        model = get_model(collection_metadata, {collection_metadata.id_key: collection_name})
        repo.delete(model.pk, model.sk)

    @staticmethod
    def get_all_collections(start_from: str = None, limit: int = None) -> (Collection, dict):
        result = QueryService.query(collection_metadata, AnyMatch(), None, limit, start_from)
        if result:
            return list(
                map(lambda m: Converter.from_dict_to_collection(m.document), result.data)), result.lastEvaluatedKey

    @staticmethod
    def get_all_collections_generator():
        has_more = True
        while has_more:
            last_evaluated_key = None
            collections, last_evaluated_key = CollectionService.get_all_collections(last_evaluated_key)
            has_more = last_evaluated_key is not None
            for c in collections:
                yield c


class IndexService:

    @staticmethod
    def get_index_by_name(name: str) -> Index:
        query_result = QueryService.query(index_metadata, Eq("name", name), index_by_name_metadata, None, 1)
        return list(
            map(lambda m: Converter.from_dict_to_index(m.document), query_result.data)), query_result.lastEvaluatedKey

    @staticmethod
    def create_index(index: Index) -> Index:
        logger.debug("index : {} ".format(index.__str__()))
        index_dict = Converter.from_index_to_dict(index)
        logger.debug("index dict : {} ".format(index_dict))

        existing_index, last_index_id = IndexService.get_index_by_name(index.index_name)

        if len(existing_index) > 0:
            return existing_index[0]

        repo = get_repository_factory(index_metadata)
        model = get_model(index_metadata, index_dict)
        create_index_model = repo.create(model)
        logger.info("index created {}".format(create_index_model.__str__()))
        if create_index_model:
            created_index = Converter.from_dict_to_index(create_index_model.document)
            index_by_collection_name_model = repo.create(
                get_index_model(index_metadata, Index("index", ["collection.name"]), index_dict))
            logger.info(
                "{} has been indexed {}".format(created_index.collection_name, index_by_collection_name_model.document))
            index_by_name_model = repo.create(
                get_index_model(index_metadata, Index("index", ["collection.name", "name"]), index_dict))
            logger.info("{} has been indexed {}".format(created_index.collection_name, index_by_name_model.document))
            return created_index

    @staticmethod
    def get_index_by_name_and_collection_name(name: str, collection_name: str):
        query_result = QueryService.query(index_metadata,
                                          And([Eq("collection.name", collection_name), Eq("name", name)]),
                                          index_by_collection_and_name_metadata, None, 1)

        indexes = list(map(lambda m: Converter.from_dict_to_index(m.document), query_result.data))
        if len(indexes) == 0:
            return None
        else:
            return indexes[0]

    @staticmethod
    def get_index_by_collection_name(collection_name: str, start_from: str = None, limit: int = 20):
        query_result = QueryService.query(index_metadata,
                                          Eq("collection.name", collection_name),
                                          index_by_collection_metadata, start_from, limit)

        return list(
            map(lambda m: Converter.from_dict_to_index(m.document), query_result.data)), query_result.lastEvaluatedKey

    @staticmethod
    def get_index_matching_fields(fields: List[str], collection_name: str, ordering_key: str = None):
        index_name = Index.index_name_generator(collection_name, fields, ordering_key)
        index = IndexService.get_index_by_name_and_collection_name(index_name, collection_name)
        fields_counter = len(fields) - 1
        while index is None and fields_counter >= 1:
            index_name = Index.index_name_generator(collection_name, fields[0:fields_counter], ordering_key)
            index = IndexService.get_index_by_name_and_collection_name(index_name, collection_name)
            fields_counter = fields_counter - 1
        return index

    @staticmethod
    def delete_index(name: str):
        repo = get_repository_factory(index_metadata)
        model = get_model(index_metadata, {index_metadata.id_key: name})
        repo.delete(model.pk, model.sk)

    @staticmethod
    def get_indexes_from_collection_name_generator(collection_name: str, limit=10):
        has_more = True
        while has_more:
            last_evaluated_key = None
            indexes, last_evaluated_key = IndexService.get_index_by_collection_name(collection_name, last_evaluated_key,
                                                                                    limit)
            has_more = last_evaluated_key is not None
            for i in indexes:
                yield i


class AuthorizationService:
    @staticmethod
    def get_client_authorization(id: str):
        repo = get_repository_factory(client_authorization_metadata)
        model = get_model(client_authorization_metadata, {client_authorization_metadata.id_key: id})
        result = repo.get(model.pk, model.sk)
        if result:
            return Converter.from_dict_to_client_authorization(result.document)

    @staticmethod
    def create_client_authorization(client_authorization: ClientAuthorization):
        client_authorization_document = Converter.from_client_authorization_to_dict(client_authorization)
        logging.info("creating client authorization {}".format(str(client_authorization)))
        repo = get_repository_factory(client_authorization_metadata)
        model = repo.create(get_model(client_authorization_metadata, client_authorization_document))
        if model:
            return Converter.from_dict_to_client_authorization(model.document)

    @staticmethod
    def update_authorization(client_authorization: ClientAuthorization):
        client_authorization_document = Converter.from_client_authorization_to_dict(client_authorization)
        logging.info("updating client authorization {}".format(str(client_authorization)))
        repo = get_repository_factory(client_authorization_metadata)
        model = repo.update(get_model(client_authorization_metadata, client_authorization_document))
        if model:
            return Converter.from_dict_to_client_authorization(model.document)

    @staticmethod
    def delete_authorization(client_id: str):
        logging.info("deleting client authorization {}".format(client_id))
        repo = get_repository_factory(client_authorization_metadata)
        model = get_model(client_authorization_metadata, {client_authorization_metadata.id_key: client_id})
        repo.delete(model.pk, model.sk)




