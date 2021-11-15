
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.v2.repository.repositories import Repository, get_table_name


def get_repository_factory(collection: Collection) -> Repository:
    return Repository(get_table_name(is_system(collection)))


def is_system(collection: Collection) -> bool:
    return collection.name in ["collection", "index", "client_authorization","aggregation_configuration","aggregation"]