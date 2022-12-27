import decimal
import unittest
import uuid
from typing import List, Callable
from unittest.mock import patch, call

from dynamoplus.models.system.aggregation.aggregation import AggregationType
from dynamoplus.models.system.client_authorization.client_authorization import ScopesType, Scope
from dynamoplus.models.system.collection.collection import AttributeDefinition, AttributeType
from dynamoplus.models.system.index.index import IndexConfiguration
from aws.dynamodb.dynamodbdao import Counter
from dynamoplus.v2.repository.repositories_v2 import IndexingOperation, QueryAll
from dynamoplus.v2.repository.system_repositories import IndexEntity, QueryIndexByCollectionNameAndFields, \
    IndexByCollectionNameAndFieldsEntity, IndexByCollectionNameEntity, QueryIndexByCollectionName, \
    ClientAuthorizationEntity, CollectionEntity, AggregationEntity, QueryAggregationByAggregationConfigurationName
from dynamoplus.v2.service.system.system_service_v2 import CollectionService, IndexService, Index, IndexEntityAdapter, \
    AuthorizationService, ClientAuthorization, ClientAuthorizationApiKey, ClientAuthorizationEntityAdapter, \
    ClientAuthorizationHttpSignature, Collection, AggregationService, AggregationCount, AggregationSum

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
        expected_collection_entity = self.build_fake_collection_entity(collection_name, id_key, ordering, attributes,
                                                                       True)
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

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_create_collection(self, dynamodb_repository_mock_factory):
        # given
        collection_name = 'book'
        id_key = "isbn"
        ordering = "date"
        attributes = [
            AttributeDefinition('title', AttributeType.STRING),
            AttributeDefinition('date', AttributeType.DATE),
        ]
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        expected_collection_entity = self.build_fake_collection_entity(collection_name, id_key, ordering, attributes,
                                                                       True)
        dynamodb_repository_mock.create.return_value = expected_collection_entity
        # when
        collection = CollectionService().create_collection(
            Collection(collection_name, id_key, ordering, attributes, True))

        # then
        self.assertIsNotNone(collection)
        self.assertEqual(collection.name, collection_name)
        # self.assertEqual(collection, Collection.from_dict(expected_collection_entity.object()))
        self.assertEqual(call('system', CollectionEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(CollectionEntity(collection_name, expected_collection_entity.object())),
                         dynamodb_repository_mock.create.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_all_collection(self, dynamodb_repository_mock_factory):
        # given

        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        expected_collection_entity_starting_from = self.build_fake_collection_entity('book', 'isbn', 'date', [
            AttributeDefinition('title', AttributeType.STRING),
            AttributeDefinition('date', AttributeType.DATE)], True)
        expected_last_evaluated_key = "booking"
        existing_index_entities = [
            self.build_fake_collection_entity('restaurant', 'id', 'insert_date', [
                AttributeDefinition('name', AttributeType.STRING),
                AttributeDefinition('address', AttributeType.STRING),
                AttributeDefinition('insert_date', AttributeType.DATE)
            ], True)
        ]
        dynamodb_repository_mock.get.return_value = expected_collection_entity_starting_from
        dynamodb_repository_mock.query.return_value = [existing_index_entities, expected_last_evaluated_key]
        # when
        results, last_evaluated_key = CollectionService().get_all_collections(20, 'book')

        # then
        self.assertIsNotNone(results)
        self.assertEqual(expected_last_evaluated_key, last_evaluated_key)
        self.assertEqual(len(results), len(existing_index_entities))
        self.assertEqual(call('system', CollectionEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(CollectionEntity('book')), dynamodb_repository_mock.get.call_args_list[0])
        self.assertEqual(call(QueryAll(CollectionEntity), 20, expected_collection_entity_starting_from),
                         dynamodb_repository_mock.query.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_delete_collection(self, dynamodb_repository_mock_factory):
        # given
        collection_name = 'book'

        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value

        # when
        CollectionService().delete_collection(collection_name)

        # then
        self.assertEqual(call('system', CollectionEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(CollectionEntity(collection_name)),
                         dynamodb_repository_mock.delete.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_aggregation_by_id(self, dynamodb_repository_mock_factory):
        # given
        uid = uuid.uuid4()
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        expected_aggregation_entity = TestSystemService.build_fake_aggregation_entity(uid,
                                                                                      'fake_count',
                                                                                      'fake_count_config',
                                                                                      lambda
                                                                                          p: TestSystemService.add_count(
                                                                                          p, 10))
        dynamodb_repository_mock.get.return_value = expected_aggregation_entity

        # when
        result = AggregationService().get_aggregation_by_id(uid)

        # then
        self.assertIsNotNone(result)
        self.assertEqual(result.id, uid)
        self.assertEqual(result.to_dict(), expected_aggregation_entity.object())
        self.assertEqual(call('system', AggregationEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(AggregationEntity(uid)),
                         dynamodb_repository_mock.get.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_aggregation_by_configuration_name(self, dynamodb_repository_mock_factory):
        # given
        starting_from = uuid.uuid4()
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        aggregation_configuration_name = 'fake_count_config'
        expected_aggregation_entity_starting_from = TestSystemService.build_fake_aggregation_entity(starting_from,
                                                                                                    'fake_count',
                                                                                                    aggregation_configuration_name,
                                                                                                    lambda
                                                                                                        p: TestSystemService.add_count(
                                                                                                        p, 10))
        expected_aggregation_entity_last_evaluated_id = uuid.uuid4()
        dynamodb_repository_mock.get.return_value = expected_aggregation_entity_starting_from
        expected_entities = [
            TestSystemService.build_fake_aggregation_entity(uuid.uuid4(),
                                                            'fake_count_1',
                                                            aggregation_configuration_name,
                                                            lambda
                                                                p: TestSystemService.add_count(
                                                                p, 10)),
            TestSystemService.build_fake_aggregation_entity(uuid.uuid4(),
                                                            'fake_count_2',
                                                            aggregation_configuration_name,
                                                            lambda
                                                                p: TestSystemService.add_avg(
                                                                p, 10))
        ]
        dynamodb_repository_mock.query.return_value = [
            expected_entities, expected_aggregation_entity_last_evaluated_id
        ]

        # when
        result, last_key = AggregationService().get_aggregations_by_configuration_name(aggregation_configuration_name,
                                                                                       20, starting_from)

        # then
        self.assertIsNotNone(result)
        self.assertEqual(len(result), len(expected_entities))
        self.assertEqual(last_key, expected_aggregation_entity_last_evaluated_id)
        self.assertEqual(call('system', AggregationEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(AggregationEntity(starting_from)),
                         dynamodb_repository_mock.get.call_args_list[0])
        self.assertEqual(call(QueryAggregationByAggregationConfigurationName(aggregation_configuration_name), 20,
                              expected_aggregation_entity_starting_from),
                         dynamodb_repository_mock.query.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_all_aggregations(self, dynamodb_repository_mock_factory):
        # given
        starting_from = uuid.uuid4()
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        aggregation_configuration_name = 'fake_count_config'
        expected_aggregation_entity_starting_from = TestSystemService.build_fake_aggregation_entity(starting_from,
                                                                                                    'fake_count',
                                                                                                    aggregation_configuration_name,
                                                                                                    lambda
                                                                                                        p: TestSystemService.add_count(
                                                                                                        p, 10))
        expected_aggregation_entity_last_evaluated_id = uuid.uuid4()
        dynamodb_repository_mock.get.return_value = expected_aggregation_entity_starting_from
        expected_entities = [
            TestSystemService.build_fake_aggregation_entity(uuid.uuid4(),
                                                            'fake_count_1',
                                                            aggregation_configuration_name,
                                                            lambda
                                                                p: TestSystemService.add_count(
                                                                p, 10)),
            TestSystemService.build_fake_aggregation_entity(uuid.uuid4(),
                                                            'fake_count_2',
                                                            aggregation_configuration_name,
                                                            lambda
                                                                p: TestSystemService.add_avg(
                                                                p, 10))
        ]
        dynamodb_repository_mock.query.return_value = [
            expected_entities, expected_aggregation_entity_last_evaluated_id
        ]

        # when
        result, last_key = AggregationService().get_all_aggregations(20, starting_from)

        # then
        self.assertIsNotNone(result)
        self.assertEqual(len(result), len(expected_entities))
        self.assertEqual(last_key, expected_aggregation_entity_last_evaluated_id)
        self.assertEqual(call('system', AggregationEntity), dynamodb_repository_mock_factory.call_args_list[0])
        self.assertEqual(call(AggregationEntity(starting_from)),
                         dynamodb_repository_mock.get.call_args_list[0])
        self.assertEqual(call(QueryAll(AggregationEntity), 20,
                              expected_aggregation_entity_starting_from),
                         dynamodb_repository_mock.query.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_increment_counter(self, dynamodb_repository_mock_factory):
        # given
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        aggregation_count = AggregationCount(uuid.uuid4(), 'count', 'count_books', 10)

        # when
        result = AggregationService().increment_count(aggregation_count)

        # then
        self.assertIsNotNone(result)
        self.assertIsInstance(result, AggregationCount)
        self.assertEqual(
            call(
                AggregationEntity(aggregation_count.id, aggregation_count.to_dict()).to_dynamo_db_model().to_dynamo_db_item(),
                [Counter("count", decimal.Decimal(1), True)]
            ),
            dynamodb_repository_mock.increment_counter.call_args_list[0]
        )

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_decrement_counter(self, dynamodb_repository_mock_factory):
        # given
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        aggregation_count = AggregationCount(uuid.uuid4(), 'count', 'count_books', 10)

        # when
        result = AggregationService().decrement_count(aggregation_count)

        # then
        self.assertIsNotNone(result)
        self.assertIsInstance(result, AggregationCount)
        self.assertEqual(
            call(
                AggregationEntity(aggregation_count.id,
                                  aggregation_count.to_dict()).to_dynamo_db_model().to_dynamo_db_item(),
                [Counter("count", decimal.Decimal(1), False)]
            ),
            dynamodb_repository_mock.increment_counter.call_args_list[0]
        )

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_increment_sum(self, dynamodb_repository_mock_factory):
        # given
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        existing_value = 10
        existing_aggregation = AggregationSum(uuid.uuid4(), 'sum', 'sum_books', existing_value)

        # when
        new_value = 5
        result = AggregationService().increment(existing_aggregation, decimal.Decimal(new_value))

        # then
        self.assertIsNotNone(result)
        self.assertIsInstance(result, AggregationSum)
        self.assertEqual(
            call(
                AggregationEntity(existing_aggregation.id,
                                  existing_aggregation.to_dict()).to_dynamo_db_model().to_dynamo_db_item(),
                [Counter("sum", decimal.Decimal(new_value-existing_value), False)]
            ),
            dynamodb_repository_mock.increment_counter.call_args_list[0]
        )

    @staticmethod
    def add_count(payload: dict, count: int) -> dict:
        return {**payload, 'count': count, 'type': AggregationType.COLLECTION_COUNT.name}

    @staticmethod
    def add_avg(payload: dict, avg: float) -> dict:
        return {**payload, 'avg': avg, 'type': AggregationType.AVG.name}

    @staticmethod
    def add_sum(payload: dict, sum: float) -> dict:
        return {**payload, 'sum': sum, 'type': AggregationType.SUM.name}

    @staticmethod
    def build_fake_aggregation_entity(uid: uuid.UUID, name: str, configuration_name: str,
                                      add_type: Callable[[dict], dict]) -> AggregationEntity:
        payload = {
            'id': uid,
            'name': name,
            'configuration_name': configuration_name,
        }
        payload = add_type(payload)
        return AggregationEntity(uid, payload)

    def build_fake_index_entity(self, name: str, fields: List[str], ordering: str, collection: str):
        uid = uuid.uuid4()
        return IndexEntity(uid, self.build_fake_index_dict(uid, name, fields, ordering, collection))

    def build_fake_collection_entity(self, collection_name: str, id_key: str,
                                     ordering: str, attributes: List[AttributeDefinition], auto_generated_id: bool):
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
