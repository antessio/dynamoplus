import json
import os
import unittest
import logging
import uuid
from datetime import datetime
from typing import *

from dynamoplus.sdk.dynamoplus_sdk import SDK, Field, AttributeType


class TestSdk(unittest.TestCase):

    def setUp(self):
        self.host = "qovok3lni2.execute-api.eu-west-1.amazonaws.com"
        self.sdk = SDK(self.host,"dev")
    def tearDown(self):
        pass

    def test_create_query(self):
        # collection = self.sdk.create_collection("book","isbn",None)
        # print(collection)
        # collection = self.sdk.create_collection("category", "id", None)
        # print(collection)
        # collection = self.sdk.get_collection("book")
        # print(collection)
        # index = self.sdk.create_index("book_name","book",[Field("name")])
        # print(index)
        # index = self.sdk.create_index("book_category", "book", [Field("category.name")],"rating")
        # print(index)
        # index = self.sdk.create_index("book_author", "book", [Field("author.name")])
        # print(index)
        # index = self.sdk.create_index("category_name", "category", [Field("name")])
        # print(index)
        # document = self.sdk.create_document("category",{
        #     "id": str(uuid.uuid4()),
        #     "name": "pulp",
        #     "ordering": "2"
        # })
        # document = self.sdk.create_document("book",
        #                                     {"isbn": str(uuid.uuid4()),
        #                                      "name":"man som hatar kvinnor",
        #                                      "category": {"name": "thriller"},
        #                                      "author": {"name":"Stig Larson"},
        #                                      "rating":"6"})
        # print(document)
        # documents = self.sdk.query_all_documents("category")
        # for d in documents:
        #     print(d)
        # documents = self.sdk.query_all_documents("book")
        # for d in documents:
        #     print(d)
        # document = self.sdk.get_document("book","11115")
        # print(document)
        # indexes = self.sdk.query_all_documents("index")
        # for i in indexes:
        #     print(i)
        # self.sdk.update_document("book")
        documents = self.sdk.query_document_by_index("book","5d5d070e-0dcd-11ea-be0d-34363bcb418c",{"category": {"name": "Pulp"}})
        for d in documents["data"]:
            print(d["isbn"]+" "+d["name"]+" "+d["category"]["name"]+" "+d["rating"])

        # self.sdk.update_document("book",d["isbn"],d)
        # document = self.sdk.get_document("book","7e4682e5-4495-446e-9522-84d921ac5240")
        # print(document)




if __name__ == '__main__':
    unittest.main()
