import logging

import uuid
from datetime import datetime
import time
from decimal import Decimal
from enum import Enum
from typing import List
import os
import dynamoplus.v2.service.system.system_service_v2
from dynamoplus.models.query.conditions import QueryCommand
from dynamoplus.models.query.index import Predicate, Eq, Range, And, Query
from dynamoplus.models.system.aggregation.aggregation import AggregationJoin, AggregationTrigger, AggregationType
from dynamoplus.models.system.client_authorization.client_authorization import ScopesType, Scope
from dynamoplus.models.system.collection.collection import AttributeDefinition, AttributeConstraint, AttributeType
from dynamoplus.models.system.index.index import IndexConfiguration
from dynamoplus.service.security.security import SecurityService
from dynamoplus.v2.repository.repositories import RepositoryInterface
from dynamoplus.v2.service.system.system_service_v2 import Collection, \
    ClientAuthorizationHttpSignature, ClientAuthorizationApiKey, ClientAuthorization, \
    ClientAuthorizationHttpSignatureCreateCommand, ClientAuthorizationApiKeyCreateCommand, \
    ClientAuthorizationCreateCommand, Index, IndexCreateCommand, AggregationConfigurationCreateCommand
from dynamoplus.v2.service.common import is_system_collection as is_system
from dynamoplus.service.validation_service import validate_collection, validate_index, validate_document, \
    validate_client_authorization, validate_aggregation_configuration, validate_query
from dynamoplus.service.indexing_decorator import create_document, update_document, delete_document
from dynamoplus.v2.service.system.system_service_v2 import AggregationConfigurationService, AggregationConfiguration, \
    Aggregation, AggregationCount, AggregationSum, AggregationAvg, AggregationService

logger = logging.getLogger()
logger.setLevel(logging.INFO)
AUTHORIZATION_FEATURE_ENABLED = bool(os.getenv("AUTHORIZATION_FEATURE_ENABLED", "False"))


class Converter:

    @staticmethod
    def convert_to_predicate(matches: Predicate) -> dynamoplus.models.query.conditions.Predicate:
        if isinstance(matches, And):
            and_predicate: And = matches
            return dynamoplus.models.query.conditions.And(
                list(
                    map(lambda eq_predicate: dynamoplus.models.query.conditions.Eq(eq_predicate.field_name,
                                                                                   eq_predicate.value),
                        filter(lambda p: isinstance(p, Eq),
                               and_predicate.predicates))
                ),
                Converter.convert_to_predicate(
                    next(filter(lambda p: not isinstance(p, Eq), and_predicate.predicates), None))
            )
        elif isinstance(matches, Eq):
            eq_predicate: Eq = matches
            return dynamoplus.models.query.conditions.Eq(eq_predicate.field_name, eq_predicate.value)
        elif isinstance(matches, Range):
            range_predicate: Range = matches
            return dynamoplus.models.query.conditions.Range(range_predicate.field_name, range_predicate.from_value,
                                                            range_predicate.to_value)

    @staticmethod
    def from_predicate_to_dict(predicate: Predicate):
        if isinstance(predicate, Eq):
            return {"eq": {"field_name": predicate.field_name, "value": predicate.value}}
        elif isinstance(predicate, Range):
            return {
                "range": {"field_name": predicate.field_name, "from": predicate.from_value, "to": predicate.to_value}}
        elif isinstance(predicate, And):
            return {"and": list(map(lambda c: Converter.from_predicate_to_dict(c), predicate.conditions))}

    @staticmethod
    def from_API_to_predicate(matches_dict: dict) -> Predicate:
        if "eq" in matches_dict:
            return Eq(matches_dict["eq"]["field_name"], matches_dict["eq"]["value"])
        elif "range" in matches_dict:
            return Range(matches_dict["range"]["field_name"], matches_dict["range"]["from"],
                         matches_dict["range"]["to"])
        elif "and" in matches_dict:
            conditions = list(map(lambda cd: Converter.from_API_to_predicate(cd), matches_dict["and"]))
            return And(conditions)

    @staticmethod
    def from_API_to_query(d: dict) -> Query:
        if "matches" in d:
            matches_dict = d["matches"]
            return Query(Converter.from_API_to_predicate(matches_dict))

    @staticmethod
    def from_collection_to_API(collection: Collection):
        result = {"name": collection.name, "id_key": collection.id_key,
                  "auto_generated_id": True if collection.auto_generated_id else False}
        if collection.ordering_key:
            result["ordering_key"] = collection.ordering_key
        if collection.attributes:
            result["attributes"] = list(
                map(lambda a: Converter.from_attribute_definition_to_API(a), collection.attributes))

        return result

    @staticmethod
    def from_attribute_definition_to_API(attribute_definition: AttributeDefinition):
        result = {"name": attribute_definition.name, "type": attribute_definition.type.name}
        if attribute_definition.attributes:
            result["attributes"] = list(
                map(lambda a: Converter.from_attribute_definition_to_API(a), attribute_definition.attributes))
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
            "auto_generate_id": collection.auto_generated_id
        }
        if collection.attributes:
            attributes = list(
                map(lambda a: Converter.from_attribute_definition_to_dict(a), collection.attributes))
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
        return ClientAuthorizationHttpSignature(d["id"] if "id" in d else None, d["client_id"], client_scopes,
                                                d["public_key"])

    @staticmethod
    def from_dict_to_client_authorization_http_signature_create_command(d: dict) -> ClientAuthorizationCreateCommand:
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationHttpSignatureCreateCommand(d["client_id"], client_scopes,
                                                             d["public_key"])

    @staticmethod
    def from_dict_to_client_authorization_api_key(d: dict):
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationApiKey(d["id"] if "id" in d else None, d["client_id"], client_scopes, d["api_key"],
                                         d["whitelist_hosts"] if "whitelist_hosts" in d else None)

    @staticmethod
    def from_dict_to_client_authorization_api_key_create_command(d: dict) -> ClientAuthorizationCreateCommand:
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationApiKeyCreateCommand(d["client_id"], client_scopes, d["api_key"],
                                                      d["whitelist_hosts"] if "whitelist_hosts" in d else None)

    @staticmethod
    def from_dict_to_client_authorization_factory():
        return {
            "api_key": Converter.from_dict_to_client_authorization_api_key,
            "http_signature": Converter.from_dict_to_client_authorization_http_signature
        }

    @staticmethod
    def from_dict_to_client_authorization_create_command_factory():
        return {
            "api_key": Converter.from_dict_to_client_authorization_api_key_create_command,
            "http_signature": Converter.from_dict_to_client_authorization_http_signature_create_command
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
    def from_dict_to_client_authorization_create_command(d: dict) -> ClientAuthorizationCreateCommand:
        return Converter.from_dict_to_client_authorization_create_command_factory()[d["type"]](d)

    @staticmethod
    def from_aggregation_configuration_to_dict(aggregation: AggregationConfiguration):
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
    def from_aggregation_configuration_to_API(aggregation_configuration: AggregationConfiguration,
                                              aggregation: Aggregation = None):
        a = {
            "on": list(map(lambda o: o.name, aggregation_configuration.on))
        }
        d = {
            "collection": {
                "name": aggregation_configuration.collection_name
            },
            "type": aggregation_configuration.type.name
        }
        if aggregation_configuration.join:
            a["join"] = {
                "collection_name": aggregation_configuration.join.collection_name,
                "using_field": aggregation_configuration.join.using_field
            }
        if aggregation_configuration.target_field:
            a["target_field"] = aggregation_configuration.target_field
        if aggregation_configuration.matches:
            a["matches"] = Converter.from_predicate_to_dict(aggregation_configuration.matches)
        d["configuration"] = a
        d["name"] = aggregation_configuration.name
        if aggregation:
            d["aggregation"] = Converter.from_aggregation_to_API(aggregation)
        return d

    @staticmethod
    def from_aggregation_to_dict(aggregation: Aggregation):
        a = {
            "name": aggregation.name,
            "configuration_name": aggregation.configuration_name
        }
        if isinstance(aggregation, AggregationCount):
            a["count"] = aggregation.count
            a["type"] = "COLLECTION_COUNT"
        if isinstance(aggregation, AggregationSum):
            a["sum"] = aggregation.sum
            a["type"] = "SUM"

        if isinstance(aggregation, AggregationAvg):
            a["avg"] = Decimal(aggregation.avg)
            a["type"] = "AVG"
        return a

    @staticmethod
    def from_aggregation_to_API(aggregation: Aggregation):
        a = {
            "name": aggregation.name
        }
        if isinstance(aggregation, AggregationCount):
            a["type"] = AggregationType.COLLECTION_COUNT.name
            a["payload"] = {
                "count": int(aggregation.count)
            }
        if isinstance(aggregation, AggregationSum):
            a["type"] = AggregationType.SUM.name
            a["payload"] = {
                "sum": int(aggregation.sum)
            }
        if isinstance(aggregation, AggregationAvg):
            a["type"] = AggregationType.AVG.name
            a["payload"] = {
                "avg": aggregation.avg
            }
        return a

    @staticmethod
    def from_dict_to_aggregation_configuration(document: dict):
        id = uuid.UUID(document['id'])
        collection_name = document["collection"]["name"]
        t = AggregationType.value_of(document["type"])
        inner_aggregation_document = document["aggregation"]
        on = list(map(lambda o: AggregationTrigger.value_of(o), inner_aggregation_document["on"]))
        target_field = None
        matches = None
        join = None
        if "target_field" in inner_aggregation_document:
            target_field = inner_aggregation_document["target_field"]
        if "join" in inner_aggregation_document:
            join = AggregationJoin(inner_aggregation_document["join"]["collection_name"],
                                   inner_aggregation_document["join"]["using_field"])
        if "matches" in inner_aggregation_document:
            matches = Converter.from_API_to_query(inner_aggregation_document["matches"])

        return AggregationConfiguration(id, collection_name, t, on, target_field, matches, join)

    @staticmethod
    def from_API_to_aggregation_configuration(document: dict):
        id = uuid.UUID(document['id'])
        collection_name = document["collection"]["name"]
        t = AggregationType.value_of(document["type"])
        inner_aggregation_document = document["configuration"]
        on = list(map(lambda o: AggregationTrigger.value_of(o), inner_aggregation_document["on"]))
        target_field = None
        matches = None
        join = None
        if "target_field" in inner_aggregation_document:
            target_field = inner_aggregation_document["target_field"]
        if "join" in inner_aggregation_document:
            join = AggregationJoin(inner_aggregation_document["join"]["collection_name"],
                                   inner_aggregation_document["join"]["using_field"])
        if "matches" in inner_aggregation_document:
            matches = Converter.from_API_to_query(inner_aggregation_document["matches"])

        return AggregationConfiguration(id, collection_name, t, on, target_field, matches, join)

    @staticmethod
    def from_dict_to_aggregation(document: dict):
        name = document["name"]
        configuraton_name = document["configuration_name"]

        if "count" in document:
            return AggregationCount(name, configuraton_name, document["count"])
        if "sum" in document:
            return AggregationSum(name, configuraton_name, document["sum"])
        if "avg" in document:
            return AggregationAvg(name, configuraton_name, document["avg"])
        return Aggregation(name, configuraton_name)


class HandlerExceptionErrorCodes(Enum):
    BAD_REQUEST = 400
    INTERNAL_SERVER_ERROR = 500
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404


class HandlerException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message


def from_API_to_aggregation_configuration_create_command(document: dict) -> AggregationConfigurationCreateCommand:
    collection_name = document["collection"]["name"]
    t = AggregationType.value_of(document["type"])
    inner_aggregation_document = document["configuration"]
    on = list(map(lambda o: AggregationTrigger.value_of(o), inner_aggregation_document["on"]))
    target_field = None
    matches = None
    join = None
    if "target_field" in inner_aggregation_document:
        target_field = inner_aggregation_document["target_field"]
    if "join" in inner_aggregation_document:
        join = AggregationJoin(inner_aggregation_document["join"]["collection_name"],
                               inner_aggregation_document["join"]["using_field"])
    if "matches" in inner_aggregation_document:
        matches = Converter.from_API_to_predicate(inner_aggregation_document["matches"])

    return AggregationConfigurationCreateCommand(collection_name, t, on, target_field, matches, join)


def from_aggregation_configuration_to_API(aggregation_configuration: AggregationConfiguration,
                                          aggregation: Aggregation = None):
    a = {
        "id": str(aggregation_configuration.uid),
        "on": list(map(lambda o: o.name, aggregation_configuration.on))
    }
    d = {
        "collection": {
            "name": aggregation_configuration.collection_name
        },
        "type": aggregation_configuration.type.name
    }
    if aggregation_configuration.join:
        a["join"] = {
            "collection_name": aggregation_configuration.join.collection_name,
            "using_field": aggregation_configuration.join.using_field
        }
    if aggregation_configuration.target_field:
        a["target_field"] = aggregation_configuration.target_field
    if aggregation_configuration.matches:
        a["matches"] = Converter.from_predicate_to_dict(aggregation_configuration.matches)
    d["configuration"] = a
    d["name"] = aggregation_configuration.name
    if aggregation:
        d["aggregation"] = from_aggregation_to_API(aggregation)
    return d


def from_aggregation_to_API(aggregation: Aggregation):
    a = {
        "id": str(aggregation.id),
        "name": aggregation.name,
        "configuration_name": aggregation.configuration_name
    }
    if isinstance(aggregation, AggregationCount):
        a["type"] = AggregationType.COLLECTION_COUNT.name
        a["payload"] = {
            "count": int(aggregation.count)
        }
    if isinstance(aggregation, AggregationSum):
        a["type"] = AggregationType.SUM.name
        a["payload"] = {
            "sum": int(aggregation.sum)
        }
    if isinstance(aggregation, AggregationAvg):
        a["type"] = AggregationType.AVG.name
        a["payload"] = {
            "avg": aggregation.avg
        }
    return a


def from_collection_to_API(collection: dynamoplus.v2.service.system.system_service_v2.Collection) -> dict:
    result = {"name": collection.name, "id_key": collection.id_key,
              "auto_generate_id": True if collection.auto_generated_id else False}
    if collection.ordering_key:
        result["ordering_key"] = collection.ordering_key
    if collection.attributes:
        result["attributes"] = list(map(lambda a: from_attribute_definition_to_API(a), collection.attributes))

    return result


def from_index_to_API(index: dynamoplus.v2.service.system.system_service_v2.Index) -> dict:
    index_dict = {
        "id": str(index.id),
        "name": index.name,
        "collection": {
            "name": index.collection_name
        },
        "conditions": index.conditions
    }
    if index.ordering_key:
        index_dict["ordering_key"] = index.ordering_key
    if index.index_configuration:
        index_dict["configuration"] = index.index_configuration.name
    return index_dict


def from_dict_to_index(d: dict) -> Index:
    return Index(
        uuid.uuid4(),
        d["collection"]["name"],
        d["conditions"],
        IndexConfiguration.value_of(d["configuration"]) if "configuration" in d else None,
        d["ordering_key"] if "ordering_key" in d else None)


def from_dict_to_create_index_command(d: dict) -> IndexCreateCommand:
    return IndexCreateCommand(
        d["collection"]["name"],
        d["conditions"],
        IndexConfiguration.value_of(d["configuration"]) if "configuration" in d else None,
        d["ordering_key"] if "ordering_key" in d else None)


def from_attribute_definition_to_API(attribute_definition: AttributeDefinition):
    result = {"name": attribute_definition.name, "type": attribute_definition.type.name}
    if attribute_definition.attributes:
        result["attributes"] = list(map(lambda a: from_attribute_definition_to_API(a), attribute_definition.attributes))
    return result


def from_dict_to_collection(d: dict):
    attributes = list(
        map(Converter.from_dict_to_attribute_definition, d["attributes"])) if "attributes" in d else None
    auto_generate_id = d["auto_generate_id"] if "auto_generate_id" in d else False
    return dynamoplus.v2.service.system.system_service_v2.Collection(d["name"], d["id_key"],
                                                                     d["ordering_key"] if "ordering_key" in d else None,
                                                                     attributes,
                                                                     auto_generate_id)


class Dynamoplus:

    def __init__(self,
                 collection_repository: RepositoryInterface,
                 index_repository: RepositoryInterface,
                 aggregation_configuration_repository: RepositoryInterface,
                 aggregation_repository: RepositoryInterface,
                 client_authorization_repository: RepositoryInterface,
                 domain_repository: RepositoryInterface):
        self.aggregation_configuration_service = AggregationConfigurationService(aggregation_configuration_repository)
        self.aggregation_service = AggregationService(aggregation_repository)
        self.index_service = dynamoplus.v2.service.system.system_service_v2.IndexService(index_repository)
        self.collection_service = dynamoplus.v2.service.system.system_service_v2.CollectionService(
            collection_repository)
        self.client_authorization_service = dynamoplus.v2.service.system.system_service_v2.AuthorizationService(
            client_authorization_repository)
        self.domain_service = dynamoplus.v2.service.domain.domain_service_v2.DomainService(domain_repository)

    def get_all(self, collection_name: str, last_key: str, limit: int):
        is_system_collection = is_system(collection_name)
        if is_system_collection:
            logger.info("Get {} metadata from system".format(collection_name))
            if collection_name == 'collection':

                collections, last_evaluated_key = self.collection_service.get_all_collections(limit, last_key)
                documents = list(map(lambda c: from_collection_to_API(c), collections))
                return documents, last_evaluated_key
            elif collection_name == 'index':
                indexes, last_evaluated_key = self.index_service.get_all_indexes(limit, uuid.UUID(
                    last_key) if last_key else None)
                documents = list(map(lambda c: from_index_to_API(c), indexes))
                return documents, last_evaluated_key
            elif collection_name == 'aggregation_configuration':
                aggregation_configuration_list, last_evaluated_key = self.aggregation_configuration_service.get_all_aggregation_configurations(
                    limit, uuid.UUID(last_key) if last_key else None)
                documents = list(map(lambda c: from_aggregation_configuration_to_API(c,
                                                                                     self.aggregation_service.get_aggregation_by_configuration_name(
                                                                                         c.name)),
                                     aggregation_configuration_list))
                return documents, last_evaluated_key
            elif collection_name == 'aggregation':
                aggregations, last_evaluated_key = self.aggregation_service.get_all_aggregations(limit, uuid.UUID(
                    last_key) if last_key else None)
                documents = list(map(lambda c: from_aggregation_to_API(c), aggregations))
                return documents, last_evaluated_key
            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} not valid collection".format(collection_name))
        else:
            logger.info("get all  {} collection limit = {} last_key = {} ".format(collection_name, limit, last_key))
            collection_metadata = self.collection_service.get_collection(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))

            logger.info("Query all {}".format(collection_name))
            documents, last_evaluated_key = self.domain_service.find_all(collection_metadata, limit, last_key)
            return documents, last_evaluated_key

    def aggregation_configurations(self, collection_name: str, last_key: str, limit: int):
        is_system_collection = is_system(collection_name)
        if is_system_collection:
            raise HandlerException(HandlerExceptionErrorCodes.FORBIDDEN,
                                   "cannot get aggregation for system collections {}".format(collection_name))
        else:
            aggregation_configurations, last_evaluated_key = self.aggregation_configuration_service.get_aggregation_configurations_by_collection_name(
                collection_name, limit, uuid.UUID(last_key) if last_key else None)
            documents = list(map(lambda c: from_aggregation_configuration_to_API(c,
                                                                                 self.aggregation_service.get_aggregation_by_configuration_name(
                                                                                     c.name)),
                                 aggregation_configurations))
            return documents, last_evaluated_key

    def get(self, collection_name: str, document_id: str):
        is_system_collection = is_system(collection_name)
        if is_system_collection:
            logger.info("Get {} metadata from system".format(collection_name))
            if collection_name == 'collection':
                collection = self.collection_service.get_collection(document_id)
                if collection is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, document_id))
                logger.info("Found collection {}".format(collection.__str__))
                return from_collection_to_API(collection)
            elif collection_name == 'index':
                index = self.index_service.get_index_by_id(uuid.UUID(document_id))

                if index is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, document_id))
                logger.info("Found index {}".format(index.__str__))
                return from_index_to_API(index)
            elif collection_name == 'client_authorization':

                client_authorization = self.client_authorization_service.get_client_authorization(
                    uuid.UUID(document_id))
                if client_authorization is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, document_id))
                logger.info("Found client_authorization {}".format(client_authorization.__str__))
                return Converter.from_client_authorization_to_dict(client_authorization)
            elif collection_name == 'aggregation_configuration':
                aggregation_configuration = self.aggregation_configuration_service.get_aggregation_configuration_by_uid(
                    uuid.UUID(document_id))
                if aggregation_configuration is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, document_id))
                logger.info("Found aggregation configuration {}".format(aggregation_configuration.__str__))
                return from_aggregation_configuration_to_API(aggregation_configuration)
            elif collection_name == 'aggregation':
                aggregation = self.aggregation_service.get_aggregation_by_id(uuid.UUID(document_id))
                if aggregation is None:
                    raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                           "{} not found with name {}".format(collection_name, document_id))
                logger.info("Found aggregation {}".format(aggregation.__str__))
                return from_aggregation_to_API(aggregation)
            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} not a valid collection".format(collection_name))

        else:
            logger.info("Get {} document".format(collection_name))
            collection = self.collection_service.get_collection(collection_name)
            if collection is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            document = self.domain_service.get_document(document_id, collection)
            if document is None:
                raise HandlerException(HandlerExceptionErrorCodes.NOT_FOUND,
                                       "{} not found with id {}".format(collection_name, document_id))
            return document

    @create_document
    def create(self, collection_name: str, document: dict) -> dict:
        is_system_collection = is_system(collection_name)
        if is_system_collection:
            logger.info("Creating {} metadata {}".format(collection_name, document))
            if collection_name == 'collection':
                validate_collection(document)
                collection = self.collection_service.create_collection(from_dict_to_collection(document))
                logger.info("Created collection {}".format(collection.__str__))
                return from_collection_to_API(collection)
            elif collection_name == 'index':
                validate_index(document)
                create_index_command = from_dict_to_create_index_command(document)
                index = self.index_service.create_index(create_index_command)
                logger.info("Created index {}".format(index))
                return from_index_to_API(index)
            elif collection_name == 'client_authorization':
                validate_client_authorization(document)
                client_authorization = Converter.from_dict_to_client_authorization_create_command(document)
                client_authorization = self.client_authorization_service.create_client_authorization(
                    client_authorization)
                logging.info("created client_authorization {}".format(client_authorization.__str__()))
                return Converter.from_client_authorization_to_dict(client_authorization)
            elif collection_name == "aggregation_configuration":
                validate_aggregation_configuration(document)
                aggregation_configuration_create_command = from_API_to_aggregation_configuration_create_command(
                    document)
                aggregation_configuration_create_command = self.aggregation_configuration_service.create_aggregation_configuration(
                    aggregation_configuration_create_command)
                logging.info("created aggregation_configuration_create_command {}".format(
                    aggregation_configuration_create_command.__str__()))
                return Converter.from_aggregation_configuration_to_API(aggregation_configuration_create_command)
            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
        else:
            logger.info("Create {} document {}".format(collection_name, document))
            collection = self.collection_service.get_collection(collection_name)
            if collection is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            validate_document(document, collection)
            timestamp = datetime.utcnow()
            ## TODO: key generator
            if collection.auto_generated_id:
                document[collection.id_key] = str(uuid.uuid1())
            document["creation_date_time"] = timestamp.isoformat()
            document["order_unique"] = str(int(time.time() * 1000.0))
            d = self.domain_service.create_document(document, collection)
            logger.info("Created document {}".format(d))
            return d

    @update_document
    def update(self, collection_name: str, document: dict, document_id: str = None):
        is_system_collection = is_system(collection_name)
        if is_system_collection:
            if collection_name == "client_authorization":
                if document_id:
                    document["client_id"] = document_id
                validate_client_authorization(document)
                client_authorization = Converter.from_dict_to_client_authorization(document)
                client_authorization = self.client_authorization_service.update_authorization(client_authorization)
                logging.info("updated client_authorization {}".format(client_authorization.__str__))
                return Converter.from_client_authorization_to_dict(client_authorization)
            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "updating {} is not supported ".format(collection_name))
        else:
            logger.info("update {} document {}".format(collection_name, document))
            collection_metadata = self.collection_service.get_collection(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            if document_id:
                document[collection_metadata.id_key] = document_id
            validate_document(document, collection_metadata)
            timestamp = datetime.utcnow()
            document["update_date_time"] = timestamp.isoformat()
            d = self.domain_service.update_document(document, collection_metadata)
            logger.info("updated document {}".format(d))
            return d

    def query(self, collection_name: str, query: dict = None, start_from: str = None,
              limit: int = None):
        is_system_collection = is_system(collection_name)
        documents = []
        if is_system_collection:
            if collection_name == 'collection':
                collections, last_key = self.collection_service.get_all_collections(limit, start_from)
                documents = list(map(lambda c: from_collection_to_API(c), collections))
                last_evaluated_key = last_key
            elif collection_name == 'index' and "matches" in query and "eq" in query["matches"] and "value" in \
                    query["matches"]["eq"]:
                target_collection_name = query["matches"]["eq"]["value"]
                index_metadata_list, last_key = self.index_service.get_index_by_collection_name(
                    target_collection_name, limit, start_from)
                documents = list(map(lambda i: Converter.from_index_to_dict(i), index_metadata_list))
                last_evaluated_key = last_key
            elif collection_name == 'client_authorization' and 'matches' in query and "eq" in query[
                "matches"] and "value" in query["matches"]["eq"]:
                client_id = query["matches"]["eq"]["value"]
                client_authorization_list, last_key = self.client_authorization_service.get_client_authorization_by_client_id(
                    client_id, limit, start_from)
                documents = list(
                    map(lambda i: Converter.from_client_authorization_to_dict(i), client_authorization_list))
                last_evaluated_key = last_key
            elif collection_name == 'aggregation':
                validate_query(query)
                query: Query = Converter.from_API_to_query(query)
                if isinstance(query.matches, Eq) and query.matches.field_name == 'configuration_name':
                    aggregation_configuration_eq_predicate: Eq = query.matches
                    aggregations, last_key = self.aggregation_service.get_aggregations_by_configuration_name(aggregation_configuration_eq_predicate.value, limit, uuid.UUID(start_from) if start_from else None)
                    documents = list(map(lambda c: from_aggregation_to_API(c), aggregations))
                    last_evaluated_key = last_key
                else:
                    raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST, "invalid query for aggregation")

            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
        else:

            if "matches" in query:
                validate_query(query)
                query: Query = Converter.from_API_to_query(query)
                # further validation
                if isinstance(query.matches, And):
                    and_condition: And = query.matches
                    non_eq_count = 0
                    for predicate in and_condition.predicates:
                        if not isinstance(predicate, Eq):
                            non_eq_count += 1
                            if non_eq_count > 1:
                                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                                       "Invalid predicate list in \"and\". At most one non-eq predicate is allowed.")

            else:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "invalid predicate")
            logger.info("query {} collection by {} ".format(collection_name, query))
            collection_metadata = self.collection_service.get_collection(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))

            ## TODO - missing order unique in the query
            index_matching_conditions = self.index_service.get_index_by_collection_name_and_conditions(collection_name,
                                                                                                       query.matches.get_field_names())

            if index_matching_conditions is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST, "no index found")
            logger.info("Found index matching {}".format(index_matching_conditions.conditions))
            query_command = QueryCommand(
                Converter.convert_to_predicate(query.matches),
                index_matching_conditions.name,
                index_matching_conditions.conditions)
            documents, last_evaluated_key = self.domain_service.query(collection_metadata, query_command, limit,
                                                                      start_from)
        return documents, last_evaluated_key

    @delete_document
    def delete(self, collection_name: str, id: str):
        is_system_collection = is_system(collection_name)
        if is_system_collection:
            logger.info("delete {} metadata from system".format(collection_name))
            if collection_name == 'collection':
                self.collection_service.delete_collection(id)
            elif collection_name == 'index':
                self.index_service.delete_index(uuid.UUID(id))
            elif collection_name == 'client_authorization':
                self.client_authorization_service.delete_authorization(uuid.UUID(id))
            else:
                raise NotImplementedError("collection_name {} not handled".format(collection_name))
        else:
            logger.info("delete {} document {}".format(collection_name, id))
            collection_metadata = self.collection_service.get_collection(collection_name)
            if collection_metadata is None:
                raise HandlerException(HandlerExceptionErrorCodes.BAD_REQUEST,
                                       "{} is not a valid collection".format(collection_name))
            self.domain_service.delete_document(id, collection_name)

    def info(self) -> dict:
        return {
            "version": "0.5.0"
        }

    def authorize(self, headers: dict, http_method: str, path: str) -> str:
        if not AUTHORIZATION_FEATURE_ENABLED:
            return 'anonymous'
        else:
            headers_dict = dict(headers)
            try:
                if SecurityService.is_bearer(headers_dict):
                    username = SecurityService.get_bearer_authorized(headers_dict)
                    logger.debug("Found {} in credentials".format(username))
                    return username
                elif SecurityService.is_basic_auth(headers_dict):
                    username = SecurityService.get_basic_auth_authorized(headers_dict)
                    logger.debug("Found {} in credentials".format(username))
                    return username
                elif SecurityService.is_api_key(headers_dict):
                    client_authorization = SecurityService.get_client_authorization_by_api_key(headers_dict,
                                                                                               self.client_authorization_service.get_client_authorization_by_client_id)
                    logger.debug("client authorization = {}".format(client_authorization))
                    if client_authorization and SecurityService.check_scope(path,
                                                                            http_method,
                                                                            client_authorization.client_scopes):
                        return client_authorization.client_id
                    else:
                        return None

                elif SecurityService.is_http_signature(headers_dict):
                    client_authorization = SecurityService.get_client_authorization_using_http_signature_authorized(
                        headers_dict, http_method.lower(), path)
                    if client_authorization and SecurityService.check_scope(path, http_method,
                                                                            client_authorization.client_scopes):
                        return client_authorization.client_id
                    else:
                        return None
            except Exception as e:
                print(f'Exception encountered: {e}')
                logger.error("exception encountered", e)
                return None
