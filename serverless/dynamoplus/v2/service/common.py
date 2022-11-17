
from dynamoplus.models.system.collection.collection import Collection
from aws.dynamodb.dynamodbdao import DynamoDBDAO, get_table_name


def get_repository_factory(collection: Collection) -> DynamoDBDAO:
    return DynamoDBDAO(get_table_name(is_system(collection)))


def is_system(collection: Collection) -> bool:
    return collection.name in ["collection", "index", "client_authorization","aggregation_configuration","aggregation"]