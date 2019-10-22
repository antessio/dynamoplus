from typing import *
import unittest
import os
import sys
from dynamoplus.repository.repositories import Repository
from dynamoplus.repository.models import Model
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.http.handler import HttpHandler

from datetime import datetime

from moto import mock_dynamodb2
import boto3
import os
import json

@mock_dynamodb2
class TestHttpHandler(unittest.TestCase):

    def setUp(self):
        os.environ["TEST_FLAG"]="true"
        os.environ["ALLOWED_ORIGINS"]="http://localhost"
        os.environ["DYNAMODB_TABLE"]="example_1"
        os.environ["ENTITIES"]="collection#id#creation_date_time,index#id#creation_date_time"
        os.environ["INDEXES"]="collection#name,index#name,index#collection.name"
        self.dynamodb = boto3.resource("dynamodb")
        self.httpHandler = HttpHandler()
        self.table = self.getMockTable()
    def tearDown(self):
        self.table.delete()
    def test_getTargetEntity(self):
        path_parameters={"collection":"example","query": "name"}
        result=self.httpHandler.getDocumentTypeFromPathParameters(path_parameters)
        self.assertEqual(result,"example")
        
    def test_get_entityNotHandled(self):
        self.fill_data(self.table)
        docTypeConfig = DocumentTypeConfiguration("example", "id", "ordering")
        expectedModel = Model(docTypeConfig,{"id": "1", "sk": "example","pk":"example#1", "data":"1", "ordering":"1", "attribute1": "value1"})
        expectedResult = {"id": "1", "attribute1":"value1","ordering":"1"}
        result = self.httpHandler.get({"entity":"example", "id":"1"})
        self.assertEqual(result["statusCode"],400)
        #self.assertEqual(result["body"], self.httpHandler.formatJson(expectedResult))

    def test_get_found(self):
        self.fill_data(self.table)
        docTypeConfig = DocumentTypeConfiguration("example", "id", "ordering")
        expectedModel = Model(docTypeConfig,{"id": "1", "sk": "example","pk":"example#1", "data":"1", "ordering":"1", "attribute1": "value1"})
        expectedResult = {"id": "1", "title":"data_1","ordering":"1"}
        result = self.httpHandler.get({"collection":"example", "id":"1"})
        self.assertEqual(result["statusCode"],200)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expectedResult,["even"])
    
    def test_create(self):
        self.fill_data(self.table)
        expectedResult = {"id": "1000", "title":"test_1","ordering":"21"}
        result = self.httpHandler.create({"collection":"example"},body="{\"id\":\"1000\", \"title\": \"test_1\",\"ordering\": \"21\"}")
        self.assertEqual(result["statusCode"],201)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expectedResult,["id","creation_date_time"])

    
    def test_update_adding_new_field(self):
        self.fill_data(self.table)
        expectedResult = {"id": "1", "title":"test_1","ordering":"21", "new_attribute": "001"}
        result = self.httpHandler.update({"collection":"example"},body="{\"id\":\"1\", \"title\": \"test_1\", \"new_attribute\": \"001\", \"ordering\": \"21\"}")
        self.assertEqual(result["statusCode"],200)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expectedResult,["creation_date_time","update_date_time"])
    def test_update_edit_existing_field(self):
        self.fill_data(self.table)
        expectedResult = {"id": "1", "title":"test_1","ordering":"21"}
        result = self.httpHandler.update({"collection":"example"},body="{\"id\":\"1\", \"title\": \"test_1\", \"ordering\": \"21\"}")
        self.assertEqual(result["statusCode"],200)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expectedResult,["creation_date_time","update_date_time"])
    def test_delete(self):
        self.fill_data(self.table)
        expectedResult = {"id": "1", "title":"test_1","ordering":"21"}
        result = self.httpHandler.delete({"collection":"example","id":"1"})
        self.assertEqual(result["statusCode"],200)
        
    def test_query(self):
        self.fill_data(self.table)
        origin="http://localhost"
        result = self.httpHandler.query({"collection": "example", "queryId": "even"}, body="{\"even\": \"1\"}",headers={"origin": origin})
        self.assertEqual(result["statusCode"],200)
        body=json.loads(result["body"])
        self.assertEqual(len(body["data"]),10)
        headers=result["headers"]
        self.assertIn("Access-Control-Allow-Origin",headers)
        self.assertEqual(origin,headers["Access-Control-Allow-Origin"])
    def test_access_control_allow_origin(self):
        self.fill_data(self.table)
        origin="http://localhost"
        result = self.httpHandler.query({"collection": "example", "queryId": "even"}, body="{\"even\": \"1\"}",headers={"origin": origin})
        self.assertEqual(result["statusCode"],200)
        body=json.loads(result["body"])
        self.assertEqual(len(body["data"]),10)
        headers=result["headers"]
        self.assertIn("Access-Control-Allow-Origin",headers)
        self.assertEqual(origin,headers["Access-Control-Allow-Origin"])
    def test_access_control_not_allow_origin(self):
        self.fill_data(self.table)
        origin="http://localhost:3000"
        result = self.httpHandler.query({"collection": "example", "queryId": "even"}, body="{\"even\": \"1\"}",headers={"origin": origin})
        self.assertEqual(result["statusCode"],200)
        body=json.loads(result["body"])
        self.assertEqual(len(body["data"]),10)
        headers=result["headers"]
        self.assertNotIn("Access-Control-Allow-Origin",headers)
    def test_query_all(self):
        self.fill_data(self.table)
        result = self.httpHandler.query({"collection": "example"}, body="{}",headers={"Origin": "http://localhost:3000"})
        self.assertEqual(result["statusCode"],200)
        body=json.loads(result["body"])
        self.assertEqual(len(body["data"]),20)


    # def test_query_not_handled(self):
    #     self.fill_data(self.table)
    #     result = self.httpHandler.query({"collection": "example", "queryId": "whatever"}, body="{\"title\": \"data_1\"}",headers={"Origin": "http://localhost:3000"})
    #     self.assertEqual(result["statusCode"],400)
    def fill_data(self,table):
        timestamp = datetime.utcnow()
        document = {"name": "example", "idKey":"id", "orderingKey": "ordering", "creation_date_time": timestamp.isoformat()}
        table.put_item(Item={"pk":"collection#1","sk":"collection","data":"1", "document":{**document}})
        table.put_item(Item={"pk":"collection#1","sk":"collection#name","data":"example", "document": {**document}})
        for i in range(1,21):
            document = {"id": str(i), "title": "data_"+str(i), "even":str(i%2), "ordering":str(i)}
            table.put_item(Item={"pk":"example#"+str(i),"sk":"example","data":str(i), "document":{**document}})
            table.put_item(Item={"pk":"example#"+str(i),"sk":"example#title","data":"data_"+str(i), "document": {**document}})
            table.put_item(Item={"pk":"example#"+str(i),"sk":"example#even","data":str(i%2), "document": {**document}})
    def getMockTable(self):
        table = self.dynamodb.create_table(TableName="example_1",
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
        print("Table status:", table.table_status)
        return table


    def assertDictEqualsIgnoringFields(self, d1:dict, d2:dict, fields:List[str]=[]):
        d1={k: v for k, v in d1.items() if k not in fields}
        d2={k: v for k, v in d2.items() if k not in fields}
        self.assertDictEqual(d1,d2)

if __name__ == '__main__':
    unittest.main()
