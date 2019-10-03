import unittest
from typing import *
from dynamoplus.repository.repositories import Model
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration

class TestRepository(unittest.TestCase):
    def test_model(self):
        pass
        # documentConfiguration = DocumentTypeConfiguration("example","id","ordering")
        # document = {"id": "randomId","ordering": "123456"}
        # model = Model(documentConfiguration,document)
        