from typing import List

from dynamoplus.models.query.conditions import Predicate, AnyMatch, get_range_predicate, Eq, And, Range
from dynamoplus.v2.repository.domain_repository import DomainEntity, QueryByField
from dynamoplus.v2.repository.repositories_v2 import DynamoDBRepository, Query, QueryAll
from dynamoplus.v2.service.system.system_service_v2 import Collection, Index


class DomainService:
    def __init__(self, collection: Collection):
        self.repo = DynamoDBRepository(DomainEntity)

    def get_document(self, id: str, collection: Collection) -> dict:
        domain_entity = self.repo.get(self.get_domain_entity_from_id(id, collection))
        if domain_entity:
            return domain_entity.object()

    def get_domain_entity_from_id(self, id: str, collection: Collection):
        return DomainEntity(id, collection.name, None)

    def create_document(self, document: dict, collection: Collection):
        entity = self.get_domain_entity_from_document(document, collection)
        created_domain_entity = self.repo.create(entity)
        if created_domain_entity:
            return created_domain_entity.object()

    def get_domain_entity_from_document(self, document: dict, collection: Collection):
        if collection.id_key not in document:
            raise ValueError(
                "document {} doesn't contain the key attribute {}".format(document, collection.id_key))
        ordering = None
        if collection.ordering and collection.ordering in document:
            ordering = document[collection.ordering]
        entity = DomainEntity(document[collection.id_key], collection.name, document, ordering)
        return entity

    def update_document(self, document: dict, collection: Collection):
        updated_entity = self.repo.update(self.get_domain_entity_from_document(document, collection))
        if updated_entity:
            return updated_entity.object()

    def delete_document(self, id: str, collection: Collection):
        self.repo.delete(self.get_domain_entity_from_id(id))

    def find_all(self, collection: Collection, limit: int = None, start_from: str = None):
        return self.query(AnyMatch(), None, limit, start_from)

    def query(self, predicate: Predicate, index: Index, limit: int = None, start_from: str = None) -> (
            List[dict], str):

        query = QueryAll(DomainEntity)
        if predicate and len(predicate.get_fields())>0:
            query = QueryByField(index.collection_name)
            query = add_to_query(query, predicate)

        result = self.repo.query(query, limit, start_from)
        return list(map(lambda dm: dm.document, result.data)), result.lastEvaluatedKey


def add_to_query(query: Query, predicate: Predicate):
    new_query: Query = query
    if isinstance(predicate, Range):
        new_query = query.add_between(predicate.get_fields()[0], predicate.get_values()[0], predicate.get_values()[1])
    elif isinstance(predicate, And):
        for c in predicate.conditions:
            new_query = add_to_query(new_query, c)
    elif isinstance(predicate, Eq):
        new_query = query.add_eq(predicate.get_fields()[0], predicate.get_values()[0])
    return new_query
