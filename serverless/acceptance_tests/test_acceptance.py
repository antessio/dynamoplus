import json
import os
import unittest
import logging
from datetime import datetime
from typing import *

from dynamoplus.sdk.dynamoplus_sdk import SDK, Field, AttributeType


class TestSdk(unittest.TestCase):

    def setUp(self):
        self.host = "7nn7292ss3.execute-api.eu-west-1.amazonaws.com"
        self.sdk = SDK(self.host,"dev")
    def tearDown(self):
        pass

    def test_create_query(self):
        # collection = self.sdk.create_collection("book","isbn","rating")
        # print(collection)
        # collection = self.sdk.get_collection("book")
        # print(collection)
        # index = self.sdk.create_index("book_name","book",[Field("name")])
        # print(index)
        document = self.sdk.create_document("book",{"isbn": "11114", "name":"Mammt2","rating":"9"})
        print(document)
        # document = self.sdk.get_document("book","1231241241")
        # print(document)




if __name__ == '__main__':
    unittest.main()
