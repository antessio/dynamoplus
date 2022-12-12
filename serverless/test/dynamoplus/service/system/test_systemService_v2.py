import unittest
import uuid
from typing import List
from unittest.mock import patch, call

import mock
import mockito.mocking

from dynamoplus.models.system.index.index import IndexConfiguration
from dynamoplus.v2.repository.repositories_v2 import DynamoDBRepository, IndexingOperation
from dynamoplus.v2.repository.system_repositories import IndexEntity, QueryIndexByCollectionNameAndFields, \
    IndexByCollectionNameAndFieldsEntity, IndexByCollectionNameEntity, QueryIndexByCollectionName
from dynamoplus.v2.service.system.system_service_v2 import IndexService, Index, IndexEntityAdapter

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
    def test_get_index_by_collection_name_and_conditions(self, dynamodb_repository_mock_factory):
        # given
        collection_name = 'book'
        fields = ['author', 'title']
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        dynamodb_repository_mock.query.return_value = [[IndexEntity(uuid.uuid4(), {"name": ("%s" % title)})], None]

        # when
        index_service = IndexService()
        index = index_service.get_index_by_collection_name_and_conditions(collection_name, fields)

        # then
        self.assertIsNotNone(index)
        self.assertEqual(call(QueryIndexByCollectionNameAndFields(collection_name,fields),1,None), dynamodb_repository_mock.query.call_args_list[0])

    @patch('dynamoplus.v2.repository.repositories_v2.DynamoDBRepository')
    def test_get_index_by_collection_name(self, dynamodb_repository_mock_factory):
        # given
        collection_name = 'book'
        limit = 20
        starting_after = uuid.uuid4()
        expected_starting_after_index = IndexEntity(starting_after, {})
        dynamodb_repository_mock = dynamodb_repository_mock_factory.return_value
        expected_repository_results = [self.build_fake_index_entity('book_by_author_and_title',['author','title'],'1','book'),
                                       self.build_fake_index_entity('book_by_author_and_year',['author','year'],'2','book')]
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
        self.assertEqual(call(IndexEntity(starting_after, None)), dynamodb_repository_mock.get.call_args_list[0])
        self.assertEqual(call(QueryIndexByCollectionName(collection_name), limit, expected_starting_after_index),
                         dynamodb_repository_mock.query.call_args_list[0])

    def build_fake_index_entity(self, name:str, fields:List[str], ordering:str, collection:str):
        uid = uuid.uuid4()
        return IndexEntity(uid, self.build_fake_index_dict(uid, name, fields, ordering,collection))

    def build_fake_index_dict(self, uid: uuid.UUID, name:str, fields:List[str], ordering: str, collection:str):
        return {"name": name, "id": str(uid),
                "conditions": fields,
                "collection":{"name": collection},
                "configuration": IndexConfiguration.OPTIMIZE_READ.name,
                "ordering_key": ordering}


