from typing import List

from dynamoplus.models.query.conditions import Predicate, AnyMatch, Eq, And, Range
from dynamoplus.v2.repository.domain_repository import DomainEntity
from dynamoplus.v2.repository.repositories_v2 import RepositoryInterface, IndexingOperation, \
    AnyCondition, Condition, BetweenCondition, EqCondition, AndCondition
from dynamoplus.v2.service.system.system_service_v2 import Collection


def get_domain_entity_from_id(id: str, collection: Collection):
    return DomainEntity(id, collection.name, None)


def get_domain_entity_from_document(document: dict, collection: Collection):
    if collection.id_key not in document:
        raise ValueError(
            "document {} doesn't contain the key attribute {}".format(document, collection.id_key))
    ordering = None
    if collection.ordering_key and collection.ordering_key in document:
        ordering = document[collection.ordering_key]
    entity = DomainEntity(document[collection.id_key], collection.name, document, ordering)
    return entity


class DomainService:
    def __init__(self, repository: RepositoryInterface):
        self.repo = repository

    def get_document(self, document_id: str, collection: Collection) -> dict:
        domain_entity = self.repo.get(document_id, collection.name)
        if domain_entity:
            return domain_entity

    def create_document(self, document: dict, collection: Collection):
        entity = get_domain_entity_from_document(document, collection)
        created_domain_entity = self.repo.create(entity)
        if created_domain_entity:
            return created_domain_entity

    def update_document(self, document: dict, collection: Collection):
        updated_entity = self.repo.update(get_domain_entity_from_document(document, collection))
        if updated_entity:
            return updated_entity

    def delete_document(self, id: str, collection: Collection):
        self.repo.delete(id, collection.name)

    def find_all(self, collection: Collection, limit: int = None, start_from: str = None):
        return self.query(collection, AnyMatch(), limit, start_from)

    def query(self, collection: Collection, predicate: Predicate, limit: int = None,
              start_from: str = None) -> (
            List[dict], str):

        condition = AnyCondition()
        if predicate and len(predicate.get_fields()) > 0:
            condition = create_condition(predicate)
        result, last_evaluated_key = self.repo.query(collection.name, condition, limit, start_from)
        return result, last_evaluated_key

    def indexing(self, indexing_operation: IndexingOperation):
        self.repo.indexing(indexing_operation)


def create_condition(predicate: Predicate) -> Condition:
    if isinstance(predicate, Range):
        return BetweenCondition(predicate.field_name, predicate.from_value, predicate.to_value)
    elif isinstance(predicate, Eq):
        return EqCondition(predicate.field_name, predicate.value)
    elif isinstance(predicate, AnyMatch):
        return AnyCondition()
    elif isinstance(predicate, And):
        eq_conditions = list(map(lambda c: EqCondition(c.field_name, c.value), predicate.eq_conditions))
        last_condition = create_condition(predicate.last_condition)
        return AndCondition(eq_conditions, last_condition)

