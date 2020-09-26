import unittest
from typing import *

from dynamoplus.models.query.conditions import Range, AnyMatch
from dynamoplus.repository.models import Model, IndexModel, QueryModel
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.query.query import Index


class TestModels(unittest.TestCase):
    def test_modelWithOrdering(self):
        # documentConfiguration = DocumentTypeConfiguration("example","id","ordering")
        collection = Collection("example", "id", "ordering")
        document = {"id": "randomId", "ordering": "123456"}
        model = Model(collection, document)
        self.assertEqual(model.pk(), "example#randomId")
        self.assertEqual(model.sk(), "example")
        self.assertEqual(model.data(), "123456")
        self.assertEqual(model.order_value(), "123456")

    def test_model(self):
        collection = Collection("example", "id", None)
        document = {"id": "randomId"}
        model = Model(collection, document)
        self.assertEqual(model.pk(), "example#randomId")
        self.assertEqual(model.sk(), "example")
        self.assertEqual(model.data(), "randomId")
        self.assertIsNone(model.order_value())

    def test_indexModel(self):
        collection = Collection("example", "id", None)
        document = {"id": "randomId", "attr1": "value2", "nested": {"condition": {"1": "value1"}}}
        index = Index("1", "example", ["nested.condition.1", "attr1"], None)
        model = IndexModel(collection, document, index)
        self.assertEqual(model.pk(), "example#randomId")
        self.assertEqual(model.sk(), "example#nested.condition.1#attr1")
        self.assertEqual(model.data(), "value1#value2")
        self.assertIsNone(model.order_value())

    def test_indexModelWithOrdering(self):
        collection = Collection("example", "id", "ordering")
        document = {"id": "randomId", "ordering": "1", "attr1": "value2", "nested": {"condition": {"1": "value1"}}}
        index = Index("1", "example", ["nested.condition.1", "attr1"], "ordering")
        model = IndexModel(collection, document, index)
        self.assertEqual(model.pk(), "example#randomId")
        self.assertEqual(model.sk(), "example#nested.condition.1#attr1")
        self.assertEqual(model.data(), "value1#value2#1")
        self.assertEqual(model.order_value(), "1")

    def test_indexModelWithNoValue(self):
        collection = Collection("example", "id", "ordering")
        document = {}
        index = None
        model = IndexModel(collection, document, None)
        self.assertEqual(model.pk(), None)
        self.assertEqual(model.sk(), "example")
        self.assertEqual(model.data(), None)
        self.assertEqual(model.order_value(), None)

    def test_collecton_metadata(self):
        collection = Collection("collection", "name")
        model = Model(collection, {"name": "example"})
        self.assertEqual(model.pk(), "collection#example")
        self.assertEqual(model.sk(), "collection")
        self.assertEqual(model.data(), "example")

    def test_query_model_range(self):
        collection = Collection("example", "id", "ordering")
        query_model = QueryModel(collection,["field1"], Range("field1","value1","value2"))
        self.assertIsNone(query_model.pk())
        self.assertEqual("example#field1", query_model.sk())
        self.assertEqual(2, len(query_model.data()))
        self.assertEqual("value1", query_model.data()[0])
        self.assertEqual("value2", query_model.data()[1])

    def test_query_model_any_match(self):
        collection = Collection("example", "id", "ordering")
        query_model = QueryModel(collection, [], AnyMatch())
        self.assertIsNone(query_model.pk())
        self.assertEqual("example", query_model.sk())
        self.assertIsNone(query_model.data())

