import unittest
import uuid
from typing import List
from unittest.mock import patch, call

from dynamoplus.models.system.client_authorization.client_authorization import ScopesType, Scope
from dynamoplus.models.system.collection.collection import AttributeDefinition, AttributeType
from dynamoplus.models.system.index.index import IndexConfiguration
from dynamoplus.v2.repository.repositories_v2 import IndexingOperation
from dynamoplus.v2.repository.system_repositories import IndexEntity, QueryIndexByCollectionNameAndFields, \
    IndexByCollectionNameAndFieldsEntity, IndexByCollectionNameEntity, QueryIndexByCollectionName, \
    ClientAuthorizationEntity, CollectionEntity
from dynamoplus.v2.service.system.system_service_v2 import CollectionService, IndexService, Index, IndexEntityAdapter, \
    AuthorizationService, ClientAuthorization, ClientAuthorizationApiKey, ClientAuthorizationEntityAdapter, \
    ClientAuthorizationHttpSignature, Collection

domain_table_name = "domain"
system_table_name = "system"


class TestSystemService(unittest.TestCase):

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_create_index(self, dynamodb_repository_mock_factory):
        # given
        expected_collection_name = 'book'
        expected_fields = ['author', 'year']
        index = Index(uuid.uuid4(), expected_collection_name, expected_fields, IndexConfiguration.OPTIMIZE_READ)
        expected_index_entity = IndexEntityAdapter(index)
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        dynamodb_repository_mock.query.return_value = [[], None]
        dynamodb_repository_mock.create.return_value = IndexEntity(index.id, index.to_dict())
        dynamodb_repository_mock.indexing.return_value = None

        # when
        index_service = IndexService()
        index_created = index_service.create_index(index)

        # then
        self.assertEqual(index_created, index)
        self.assertEqual(call('system', IndexEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(QueryIndexByCollectionNameAndFields(expected_collection_name, expected_fields), 1, None),
                         dynamodb_repository_mock.query.call_args_list[0])
        self.assertEqual(call(expected_index_entity),
                         dynamodb_repository_mock.create.call_args_list[0])
        self.assertEqual(call(IndexingOperation([],
                                                [],
                                                [
                                                    IndexByCollectionNameAndFieldsEntity(
                                                        index.id, index.collection_name,
                                                        index.conditions,
                                                        expected_index_entity.object(),
                                                        expected_index_entity.ordering()),
                                                    IndexByCollectionNameEntity(index.id, index.collection_name,
                                                                                expected_index_entity.object(),
                                                                                expected_index_entity.ordering())])),
                         dynamodb_repository_mock.indexing.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_create_index_already_exists(self, dynamodb_repository_mock_factory):
        # given
        expected_collection_name = 'book'
        expected_fields = ['author', 'year']
        index = Index(uuid.uuid4(), expected_collection_name, expected_fields, IndexConfiguration.OPTIMIZE_READ)
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        existing_index_entity = self.build_fake_index_entity('book_by_author_and_year', ['author', 'year'], '1', 'book')
        dynamodb_repository_mock.query.return_value = [[existing_index_entity], None]

        # when
        index_service = IndexService()
        index_created = index_service.create_index(index)

        # then
        self.assertNotEqual(index_created, index)
        self.assertEqual(uuid.UUID(existing_index_entity.id()), index_created.id)
        self.assertEqual(call('system', IndexEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(QueryIndexByCollectionNameAndFields(expected_collection_name, expected_fields), 1, None),
                         dynamodb_repository_mock.query.call_args_list[0])
        self.assertFalse(dynamodb_repository_mock.create.called)
        self.assertFalse(dynamodb_repository_mock.create.indexing.called)

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_index_by_collection_name_and_conditions(self, dynamodb_repository_mock_factory):
        # given
        collection_name = 'book'
        fields = ['author', 'title']
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        dynamodb_repository_mock.query.return_value = [[
            self.build_fake_index_entity('book_by_author_and_title', ['author', 'title'], '1', 'book')], None
        ]

        # when
        index_service = IndexService()
        index = index_service.get_index_by_collection_name_and_conditions(collection_name, fields)

        # then
        self.assertIsNotNone(index)
        self.assertEqual(call('system', IndexEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(QueryIndexByCollectionNameAndFields(collection_name, fields), 1, None),
                         dynamodb_repository_mock.query.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_index_by_collection_name(self, dynamodb_repository_mock_factory):
        # given
        collection_name = 'book'
        limit = 20
        starting_after = uuid.uuid4()
        expected_starting_after_index = IndexEntity(starting_after, {})
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        expected_repository_results = [
            self.build_fake_index_entity('book_by_author_and_title', ['author', 'title'], '1', 'book'),
            self.build_fake_index_entity('book_by_author_and_year', ['author', 'year'], '2', 'book')]
        dynamodb_repository_mock.query.return_value = [
            expected_repository_results, str(expected_repository_results[-1].uid)]
        dynamodb_repository_mock.get.return_value = expected_starting_after_index
        # when
        index_service = IndexService()
        result, last_key = index_service.get_index_by_collection_name(collection_name, limit, starting_after)

        # then
        self.assertIsNotNone(result)
        self.assertIsNotNone(last_key)
        self.assertEqual(2, len(result))
        self.assertEqual(call('system', IndexEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(IndexEntity(starting_after, None)), dynamodb_repository_mock.get.call_args_list[0])
        self.assertEqual(call(QueryIndexByCollectionName(collection_name), limit, expected_starting_after_index),
                         dynamodb_repository_mock.query.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_index_matching_field_all_fields(self, dynamodb_repository_mock_factory):
        # given
        collection_name = 'book'
        fields = ['author', 'title']
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        dynamodb_repository_mock.query.return_value = [
            [self.build_fake_index_entity('book_by_author_and_title', ['author', 'title'], '1', 'book')], None]

        # when
        index_service = IndexService()
        index = index_service.get_index_matching_fields(fields, collection_name)

        # then
        self.assertIsNotNone(index)
        self.assertEqual(call('system', IndexEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(QueryIndexByCollectionNameAndFields(collection_name, fields), 1, None),
                         dynamodb_repository_mock.query.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_index_matching_field_one_field(self, dynamodb_repository_mock_factory):
        # given
        collection_name = 'book'
        fields = ['author', 'title']
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        dynamodb_repository_mock.query.side_effect = [
            ([], None),
            ([self.build_fake_index_entity('book_by_author', ['author'], '1', 'book')], None)
        ]

        # when
        index_service = IndexService()
        index = index_service.get_index_matching_fields(fields, collection_name)

        # then
        self.assertIsNotNone(index)
        self.assertEqual(call('system', IndexEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(QueryIndexByCollectionNameAndFields(collection_name, fields), 1, None),
                         dynamodb_repository_mock.query.call_args_list[0])
        self.assertEqual(call(QueryIndexByCollectionNameAndFields(collection_name, ['author']), 1, None),
                         dynamodb_repository_mock.query.call_args_list[1])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_delete_index(self, dynamodb_repository_mock_factory):
        # given
        index_id = uuid.uuid4()
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value

        # when
        IndexService().delete_index(index_id)

        # then
        self.assertEqual(call('system', IndexEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(IndexEntity(index_id)), dynamodb_repository_mock.delete.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_client_authorization_api_key(self, dynamodb_repository_mock_factory):
        # given
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        expected_client_authorization_id = uuid.uuid4()
        collection_name = 'book'
        whitelist_hosts = ['http://localhost']
        api_key = 'api_key'
        client_authorization_entity = self.build_fake_client_authorization_api_key(expected_client_authorization_id,
                                                                                   collection_name,
                                                                                   whitelist_hosts, api_key)
        dynamodb_repository_mock.get.return_value = client_authorization_entity
        # when
        client_authorization = AuthorizationService().get_client_authorization(expected_client_authorization_id)

        # then
        self.assertIsNotNone(client_authorization)
        self.assertIsInstance(client_authorization, ClientAuthorizationApiKey)
        self.assertEqual(client_authorization.client_id, expected_client_authorization_id)
        # self.assertEqual(client_authorization, ClientAuthorization.from_dict(client_authorization.to_dict()))
        self.assertEqual(call('system', ClientAuthorizationEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(ClientAuthorizationEntity(expected_client_authorization_id)),
                         dynamodb_repository_mock.get.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_client_authorization_http_signature(self, dynamodb_repository_mock_factory):
        # given
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        expected_client_authorization_id = uuid.uuid4()
        collection_name = 'book'
        public_key = 'public_key'
        client_authorization_entity = self.build_fake_client_authorization_http_signature(
            expected_client_authorization_id,
            collection_name,
            public_key)
        dynamodb_repository_mock.get.return_value = client_authorization_entity
        # when
        client_authorization = AuthorizationService().get_client_authorization(expected_client_authorization_id)

        # then
        self.assertIsNotNone(client_authorization)
        self.assertIsInstance(client_authorization, ClientAuthorizationHttpSignature)
        self.assertEqual(client_authorization.client_id, expected_client_authorization_id)
        # self.assertEqual(client_authorization, ClientAuthorization.from_dict(client_authorization.to_dict()))
        self.assertEqual(call('system', ClientAuthorizationEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(ClientAuthorizationEntity(expected_client_authorization_id)),
                         dynamodb_repository_mock.get.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_create_client_authorization_api_key(self, dynamodb_repository_mock_factory):
        # given
        client_id = uuid.uuid4()
        collection_name = 'book'
        scopes = [Scope(collection_name, ScopesType.CREATE)]
        api_key = 'api_key'
        whitelist_hosts = ['http://localhost:8080']
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        expected_entity = self.build_fake_client_authorization_api_key(client_id, collection_name, whitelist_hosts,
                                                                       api_key)
        dynamodb_repository_mock.create.return_value = expected_entity

        # when
        authorization_api_key = ClientAuthorizationApiKey(client_id, scopes, api_key, whitelist_hosts)
        created_client_authorization_api_key = AuthorizationService().create_client_authorization(authorization_api_key)

        # then
        self.assertIsNotNone(created_client_authorization_api_key)
        self.assertEqual(str(created_client_authorization_api_key.client_id), expected_entity.id())
        self.assertEqual(call('system', ClientAuthorizationEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(ClientAuthorizationEntityAdapter(authorization_api_key)),
                         dynamodb_repository_mock.create.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_create_client_authorization_http_signature(self, dynamodb_repository_mock_factory):
        # given
        client_id = uuid.uuid4()
        collection_name = 'book'
        scopes = [Scope(collection_name, ScopesType.CREATE)]
        public_key = 'api_key'
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        expected_entity = self.build_fake_client_authorization_http_signature(client_id, collection_name, public_key)
        dynamodb_repository_mock.create.return_value = expected_entity

        # when
        authorization_http_signature = ClientAuthorizationHttpSignature(client_id, scopes, public_key)
        created_client_authorization_http_signature = AuthorizationService().create_client_authorization(
            authorization_http_signature)

        # then
        self.assertIsNotNone(created_client_authorization_http_signature)
        self.assertEqual(str(created_client_authorization_http_signature.client_id), expected_entity.id())
        self.assertEqual(call('system', ClientAuthorizationEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(ClientAuthorizationEntityAdapter(authorization_http_signature)),
                         dynamodb_repository_mock.create.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_update_client_authorization(self, dynamodb_repository_mock_factory):
        # given
        client_id = uuid.uuid4()
        collection_name = 'book'
        scopes = [Scope(collection_name, ScopesType.CREATE)]
        public_key = 'api_key'
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        expected_entity = self.build_fake_client_authorization_http_signature(client_id, collection_name, public_key)
        dynamodb_repository_mock.update.return_value = expected_entity

        # when
        authorization_http_signature = ClientAuthorizationHttpSignature(client_id, scopes, public_key)
        created_client_authorization_http_signature = AuthorizationService().update_authorization(
            authorization_http_signature)

        # then
        self.assertIsNotNone(created_client_authorization_http_signature)
        self.assertEqual(str(created_client_authorization_http_signature.client_id), expected_entity.id())
        self.assertEqual(call('system', ClientAuthorizationEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(ClientAuthorizationEntityAdapter(authorization_http_signature)),
                         dynamodb_repository_mock.update.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_delete_client_authorization(self, dynamodb_repository_mock_factory):
        # given
        client_id = uuid.uuid4()
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value

        # when
        AuthorizationService().delete_authorization(client_id)

        # then
        self.assertEqual(call('system', ClientAuthorizationEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(ClientAuthorizationEntity(client_id)),
                         dynamodb_repository_mock.delete.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_collection(self, dynamodb_repository_mock_factory):
        # given
        collection_name = 'book'
        id_key = "isbn"
        ordering = "date"
        attributes = [
            AttributeDefinition('title', AttributeType.STRING),
            AttributeDefinition('date', AttributeType.DATE),
        ]
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        expected_collection_entity = self.build_fake_collection_entity(collection_name, id_key, ordering, attributes, True)
        dynamodb_repository_mock.get.return_value = expected_collection_entity
        # when
        collection = CollectionService().get_collection(collection_name)

        # then
        self.assertIsNotNone(collection)
        self.assertEqual(collection.name, collection_name)
        # self.assertEqual(collection, Collection.from_dict(expected_collection_entity.object()))
        self.assertEqual(call('system', CollectionEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(CollectionEntity(collection_name)),
                         dynamodb_repository_mock.get.call_args_list[0])

    def build_fake_index_entity(self, name: str, fields: List[str], ordering: str, collection: str):
        uid = uuid.uuid4()
        return IndexEntity(uid, self.build_fake_index_dict(uid, name, fields, ordering, collection))

    def build_fake_collection_entity(self, collection_name: str, id_key: str,
                                     ordering: str, attributes: List[AttributeDefinition], auto_generated_id: bool):
        # attributes = list(
        #     map(Collection.from_dict_to_attribute_definition, d["attributes"])) if "attributes" in d else None
        # auto_generate_id = d["auto_generate_id"] if "auto_generate_id" in d else False
        # return Collection(d["name"], d["id_key"], d["ordering"] if "ordering" in d else None, attributes,
        #                   auto_generate_id)
        # attributes = None
        # if "attributes" in d and d["attributes"] is not None:
        #     attributes = list(map(Collection.from_dict_to_attribute_definition, d["attributes"]))
        # return AttributeDefinition(d["name"], Collection.from_string_to_attribute_type(d["type"]),
        #                            Collection.from_array_to_constraints_list(
        #                                d["constraints"]) if "constraints" in d else None,
        #                            attributes)

        return CollectionEntity(collection_name, {
            "name": collection_name,
            "id_key": id_key,
            "ordering": ordering,
            "auto_generate_id": auto_generated_id,
            "attributes": list(map(lambda a: {
                "name": a.name,
                "type": a.type.name
            }, attributes))
        })

    def build_fake_client_authorization_api_key(self, uid: uuid.UUID, collection_name: str, whitelist_hosts: List[str],
                                                api_key: str):
        payload = {
            "type": "api_key",
            "client_id": str(uid),
            "client_scopes": [
                {
                    "scope_type": ScopesType.CREATE.name,
                    "collection_name": collection_name
                },
                {
                    "scope_type": ScopesType.UPDATE.name,
                    "collection_name": collection_name
                },
                {
                    "scope_type": ScopesType.QUERY.name,
                    "collection_name": collection_name
                },
                {
                    "scope_type": ScopesType.DELETE.name,
                    "collection_name": collection_name
                },
                {
                    "scope_type": ScopesType.GET.name,
                    "collection_name": collection_name
                }
            ],
            "whitelist_hosts": whitelist_hosts,
            "api_key": api_key,
        }

        return ClientAuthorizationEntity(uid, payload)

    def build_fake_client_authorization_http_signature(self, uid: uuid.UUID, collection_name: str, public_key: str):
        payload = {
            "type": "http_signature",
            "client_id": str(uid),
            "client_scopes": [
                {
                    "scope_type": ScopesType.CREATE.name,
                    "collection_name": collection_name
                },
                {
                    "scope_type": ScopesType.UPDATE.name,
                    "collection_name": collection_name
                },
                {
                    "scope_type": ScopesType.QUERY.name,
                    "collection_name": collection_name
                },
                {
                    "scope_type": ScopesType.DELETE.name,
                    "collection_name": collection_name
                },
                {
                    "scope_type": ScopesType.GET.name,
                    "collection_name": collection_name
                }
            ],
            "public_key": public_key,
        }

        return ClientAuthorizationEntity(uid, payload)

    def build_fake_index_dict(self, uid: uuid.UUID, name: str, fields: List[str], ordering: str, collection: str):
        return {"name": name, "id": str(uid),
                "conditions": fields,
                "collection": {"name": collection},
                "configuration": IndexConfiguration.OPTIMIZE_READ.name,
                "ordering_key": ordering}
