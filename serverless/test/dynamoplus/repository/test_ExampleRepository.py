import unittest
import boto3
from moto import mock_dynamodb2,mock_dynamodb
#from dynamoplus.repository.Repository import Repository, _convertToString,_sanitize
from dynamoplus.repository.Repository import Repository,IndexRepository, _convertToString, _sanitize
import json
import os
import sys
import uuid
import decimal
from decimal import Decimal
from datetime import datetime

from os.path import abspath, exists
from jsonschema import ValidationError

@mock_dynamodb2
class TestRepository(unittest.TestCase):
    
    @mock_dynamodb2
    def setUp(self):
        self.dynamodb = boto3.resource("dynamodb")
        self.dynamodb.create_table(TableName="example",
            KeySchema=[
                {'AttributeName': 'pk','KeyType': 'HASH'},
                {'AttributeName': 'sk','KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                    {'AttributeName':'pk','AttributeType':'S'},
                    {'AttributeName':'sk','AttributeType':'S'},
                    {'AttributeName':'data','AttributeType':'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'sk-data-index',
                    'KeySchema': [{'AttributeName': 'sk','KeyType': 'HASH'},{'AttributeName': 'data','KeyType': 'RANGE'}],
                    "Projection":{"ProjectionType": "ALL"}
                }
            ]
        )
        self.repository = Repository("example","example","id","creation_date_time",self.dynamodb)
        f_path = abspath("test/dynamoplus/repository/test_example_payload.json")
        with open(f_path, 'r') as json_file:
            data = json.load(json_file)
        self.timestamp = datetime.utcnow()
        self.entity = { "id": "testUid", "creation_date_time": self.timestamp, **data}
    
    def tearDown(self):
        table = self.dynamodb.Table('example')
        table.delete()

    
    def test_create(self):        
        obj = self.repository.create(self.entity)
        self.assertIsNotNone(obj)
        response = self.dynamodb.Table("example").get_item(
            Key={
                'pk': "example#"+obj["id"],
                'sk': "example"
            })
        self.assertTrue("Item" in response,"Response contains no item")
    
    def test_getById(self):
        self.entity["pk"]="example#"+self.entity["id"]
        self.entity["sk"]="example"
        self.entity["data"]=str(Decimal(datetime.timestamp(self.timestamp)))        
        table = self.dynamodb.Table('example')
        table.put_item(Item=_sanitize(self.entity))
        result = self.repository.get("testUid")
        self.assertIsNotNone(result, "no result found")
        self.assertEqual(result["id"],"testUid")
        self.assertEqual(result["pk"],"example#testUid")
        self.assertEqual(result["sk"],"example")
    def test_update(self):
        self.entity["pk"]="example#"+self.entity["id"]
        self.entity["sk"]="example"
        self.entity["data"]=str(Decimal(datetime.timestamp(self.timestamp)))
        table = self.dynamodb.Table('example')
        table.put_item(Item=_sanitize(self.entity))
        self.entity["description"]="updatedDescription"
        result = self.repository.update(self.entity)
        self.assertIsNotNone(result)
        self.assertEqual(result["description"],"updatedDescription")
    def test_mainRepository(self):
        primaryKey = self.repository.getPrimaryKey(self.entity)
        self.assertEqual(primaryKey,"example#testUid")
        secondaryKey = self.repository.getSecondaryKey()
        self.assertEqual(secondaryKey, "example")
        data = self.repository.getData(self.entity)
        self.assertIsNotNone(data)
        self.assertEqual(data,str(Decimal(datetime.timestamp(self.timestamp))), "expected creation date time but is "+data)
    def test_indexRepository(self):
        testData={
            "id": "randomId",
            "attr1": True,
            "attr2": "antessio7@gmail.com",
            "attr3": decimal.Decimal("10"),
            "attr4": {
                "sub1": {
                    "sub12":{
                        "id": 1
                    }
                }
            }
        }
        ## index by attr1
        self.repository = IndexRepository("example","example","id","attr3",["attr1"],self.dynamodb)
        primaryKey = self.repository.getPrimaryKey(testData)
        self.assertEqual(primaryKey,"example#randomId")
        secondaryKey = self.repository.getSecondaryKey()
        self.assertEqual(secondaryKey, "example#attr1")
        data = self.repository.getData(testData)
        self.assertIsNotNone(data)
        self.assertEqual(data,"true#10", "expected true#10 "+data)
        ## index by attr2
        self.repository = IndexRepository("example","example","id",None,["attr2"],self.dynamodb)
        primaryKey = self.repository.getPrimaryKey(testData)
        self.assertEqual(primaryKey,"example#randomId")
        secondaryKey = self.repository.getSecondaryKey()
        self.assertEqual(secondaryKey, "example#attr2")
        data = self.repository.getData(testData)
        self.assertIsNotNone(data)
        self.assertEqual(data,"antessio7@gmail.com", "expected antessio7@gmail.com "+data)
    def test_getEntityDTO(self):
        data={}
        data["pk"]="pktest"
        data["data"]="datatest"
        data["sk"]="sktest"
        data["id"]="idtest"
        dto = self.repository.getEntityDTO(data)
        self.assertFalse("pk" in dto.keys(), "expected dto to not contain pk but was "+dto.__str__())
        self.assertFalse("sk" in dto.keys(), "expected dto to not contain sk but was "+dto.__str__())
        self.assertFalse("data" in dto.keys(), "expected dto to not contain data but was "+dto.__str__())
        
if __name__ == '__main__':
    unittest.main()
