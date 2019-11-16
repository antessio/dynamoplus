import json
import os
import unittest
import logging
from datetime import datetime
from typing import *

from dynamoplus.sdk.dynamoplus_sdk import SDK


class TestSdk(unittest.TestCase):

    def setUp(self):
        self.host = "9rod3ssp7c.execute-api.eu-west-1.amazonaws.com"
        self.sdk = SDK(self.host,"dev")
    def tearDown(self):
        pass

    def test_create_query(self):
        #collection = self.sdk.create_collection("category","id","ordering")
        collection = self.sdk.get_collection("category")
        print(collection)



if __name__ == '__main__':
    unittest.main()
