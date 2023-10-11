import os
from dataclasses import dataclass

from dynamoplus.v2.repository.repositories_v2 import Model, Query, IndexModel


@dataclass(frozen=True)
class DomainEntity(Model):
    key: str
    collection_name: str = None
    payload: dict = None
    ordering_value: str = None

    @classmethod
    def from_dict(cls, document: dict) -> Model:
        pass

    def id(self) -> str:
        return self.key

    def entity_name(self):
        return self.collection_name

    def ordering(self) -> str:
        return self.ordering_value

    def object(self) -> dict:
        return self.payload


@dataclass(frozen=True)
class IndexDomainEntity(IndexModel):
    collection_name: str
    entity_id: str
    index_entity_name: str
    index_entity_value: str
    index_entity_document: dict

    def id(self):
        return self.entity_id

    def entity_name(self):
        return self.collection_name

    def index_name(self):
        return self.index_entity_name

    def index_value(self):
        return self.index_entity_value

    def document(self):
        return self.index_entity_document
