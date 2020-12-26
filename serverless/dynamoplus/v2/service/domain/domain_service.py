# from dynamoplus.models.query.conditions import Predicate, AnyMatch
# from dynamoplus.models.system.collection.collection import Collection
# from dynamoplus.models.system.index.index import Index
# from dynamoplus.service.indexing_decorator import create_document, update_document, delete_document
# from dynamoplus.v2.service.model_service import get_model
# from dynamoplus.v2.service.system.system_service import QueryService
# from dynamoplus.v2.service.common import get_repository_factory
from dynamoplus.models.query.conditions import AnyMatch, Predicate
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.system.index.index import Index
from dynamoplus.service.indexing_decorator import create_document, update_document, delete_document
from dynamoplus.v2.service.common import get_repository_factory
from dynamoplus.v2.service.model_service import get_model
from dynamoplus.v2.service.query_service import QueryService


class DomainService:
    def __init__(self, collection: Collection):
        self.collection = collection

    def get_document(self, id: str):
        model = get_model(self.collection, {self.collection.id_key: id})
        data_model = get_repository_factory(self.collection).get(model.pk, model.sk)
        if data_model:
            return data_model.document

    @create_document
    def create_document(self, document: dict):
        model = get_model(self.collection, document)
        created_data_model = get_repository_factory(self.collection).create(model)
        if created_data_model:
            return created_data_model.document

    @update_document
    def update_document(self, document: dict):
        model = get_model(self.collection, document)
        updated_data_model = get_repository_factory(self.collection).update(model)
        if updated_data_model:
            return updated_data_model.document

    @delete_document
    def delete_document(self, id: str):
        model = get_model(self.collection, {self.collection.id_key: id})
        get_repository_factory(self.collection).delete(model.pk, model.sk)

    def find_all(self, limit: int = None, start_from: str = None):
        return self.query(AnyMatch(), None, limit, start_from)

    def query(self, predicate: Predicate, index: Index, limit: int = None, start_from: str = None):
        result = QueryService.query(self.collection,predicate,index,start_from,limit)
        return list(map(lambda dm: dm.document, result.data)), result.lastEvaluatedKey
