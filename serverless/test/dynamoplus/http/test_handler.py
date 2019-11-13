import json
import os
import unittest
import logging
from datetime import datetime
from typing import *

import boto3
from moto import mock_dynamodb2

from dynamoplus.http.handler.handler import HttpHandler




@mock_dynamodb2
class TestHttpHandler(unittest.TestCase):

    def setUp(self):
        os.environ["TEST_FLAG"] = "true"
        os.environ["ALLOWED_ORIGINS"] = "http://localhost"
        os.environ["DYNAMODB_DOMAIN_TABLE"] = "example-domain"
        os.environ["DYNAMODB_SYSTEM_TABLE"] = "example-system"
        self.dynamodb = boto3.resource("dynamodb", region_name='eu-west-1')
        os.environ["ENTITIES"] = "collection#id#creation_date_time,index#id#creation_date_time"
        os.environ["INDEXES"] = "collection#name,index#name,index#collection.name"
        self.httpHandler = HttpHandler()
        self.systemTable = self.getMockTable("example-system")
        self.table = self.getMockTable("example-domain")

    def tearDown(self):
        self.table.delete()
        self.systemTable.delete()

    def getMockTable(self, tableName):
        table = self.dynamodb.create_table(TableName=tableName,
                                           KeySchema=[
                                               {'AttributeName': 'pk', 'KeyType': 'HASH'},
                                               {'AttributeName': 'sk', 'KeyType': 'RANGE'}
                                           ],
                                           AttributeDefinitions=[
                                               {'AttributeName': 'pk', 'AttributeType': 'S'},
                                               {'AttributeName': 'sk', 'AttributeType': 'S'},
                                               {'AttributeName': 'data', 'AttributeType': 'S'}
                                           ],
                                           GlobalSecondaryIndexes=[
                                               {
                                                   'IndexName': 'sk-data-index',
                                                   'KeySchema': [{'AttributeName': 'sk', 'KeyType': 'HASH'},
                                                                 {'AttributeName': 'data', 'KeyType': 'RANGE'}],
                                                   "Projection": {"ProjectionType": "ALL"}
                                               }
                                           ]
                                           )
        print("Table status:", table.table_status)
        return table

    def fill_sytem_data(self):
        self.systemTable.put_item(Item={"pk": "collection#example", "sk": "collection", "data": "example",
                                        "document": "{\"id_key\":\"id\",\"name\":\"example\",\"fields\": [{\"field1\": \"string\"}, {\"field2.field21\": \"string\"}]}"})
        self.systemTable.put_item(Item={"pk": "index#collection.name", "sk": "index", "data": "example",
                                        "document": "{\"name\":\"collection.name\",\"collection\":{\"id_key\":\"id\",\"name\":\"example\"},\"fields\": [{\"field1\": \"string\"}, {\"field2.field21\": \"string\"}]}"})
        self.systemTable.put_item(Item={"pk": "index#even", "sk": "index", "data": "even",
                                        "document": "{\"name\":\"even\",\"collection\":{\"id_key\":\"id\",\"name\":\"example\"},\"conditions\": [\"even\"]}"})

    def fill_data(self):
        timestamp = datetime.utcnow()
        document = {"name": "example", "id_key": "id", "ordering_key": "ordering",
                    "creation_date_time": timestamp.isoformat()}
        for i in range(1, 21):
            document = {"id": str(i), "title": "data_" + str(i), "even": str(i % 2), "ordering": str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#title", "data": "data_" + str(i),
                                      "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#even", "data": str(i % 2),
                                      "document": json.dumps(document)})

    def test_getTargetEntity(self):
        path_parameters = {"collection": "example", "query": "name"}
        result = self.httpHandler.get_document_type_from_path_parameters(path_parameters)
        self.assertEqual(result, "example")

    def test_get_entityNotHandled(self):
        self.fill_sytem_data()
        self.fill_data()
        result = self.httpHandler.get({"collection":"whatever", "id":"1"})
        self.assertEqual(result["statusCode"],400)
        #self.assertEqual(result["body"], self.httpHandler.formatJson(expectedResult))

    def test_get_found(self):
        self.fill_sytem_data()
        self.fill_data()
        expected_result = {"id": "1", "title": "data_1", "ordering": "1"}
        result = self.httpHandler.get({"collection": "example", "id": "1"})
        self.assertEqual(result["statusCode"], 200)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expected_result, ["even"])

    def test_create(self):
        self.fill_sytem_data()
        self.fill_data()
        expected_result = {"id": "1000", "title": "test_1", "ordering": "21"}
        result = self.httpHandler.create({"collection": "example"},
                                         body="{\"id\":\"1000\", \"title\": \"test_1\",\"ordering\": \"21\"}")
        self.assertEqual(result["statusCode"], 201)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expected_result, ["id", "creation_date_time"])

    def test_update_adding_new_field(self):
        self.fill_sytem_data()
        self.fill_data()
        expected_result = {"id": "1", "title": "test_1", "ordering": "21", "new_attribute": "001"}
        result = self.httpHandler.update({"collection": "example"}, body="{\"id\":\"1\", \"title\": \"test_1\", "
                                                                         "\"new_attribute\": \"001\", \"ordering\": "
                                                                         "\"21\"}")
        self.assertEqual(result["statusCode"], 200)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expected_result,
                                            ["creation_date_time", "update_date_time"])

    def test_update_edit_existing_field(self):
        self.fill_sytem_data()
        self.fill_data()
        expected_result = {"id": "1", "title": "test_1", "ordering": "21"}
        result = self.httpHandler.update({"collection": "example"},
                                         body="{\"id\":\"1\", \"title\": \"test_1\", \"ordering\": \"21\"}")
        self.assertEqual(result["statusCode"], 200)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expected_result,
                                            ["creation_date_time", "update_date_time"])

    def test_delete(self):
        self.fill_sytem_data()
        self.fill_data()
        result = self.httpHandler.delete({"collection":"example","id":"1"})
        self.assertEqual(result["statusCode"],200)

    def test_query(self):
        self.fill_sytem_data()
        self.fill_data()
        origin="http://localhost"
        result = self.httpHandler.query({"collection": "example", "queryId": "even"}, body="{\"even\": \"1\"}",headers={"origin": origin})
        self.assertEqual(result["statusCode"],200)
        body=json.loads(result["body"])
        self.assertEqual(len(body["data"]),10)
        headers=result["headers"]
        self.assertIn("Access-Control-Allow-Origin",headers)
        self.assertEqual(origin,headers["Access-Control-Allow-Origin"])
    def test_access_control_allow_origin(self):
        self.fill_sytem_data()
        self.fill_data()
        origin="http://localhost"
        result = self.httpHandler.query({"collection": "example", "queryId": "even"}, body="{\"even\": \"1\"}",headers={"origin": origin})
        self.assertEqual(result["statusCode"],200)
        body=json.loads(result["body"])
        self.assertEqual(len(body["data"]),10)
        headers=result["headers"]
        self.assertIn("Access-Control-Allow-Origin",headers)
        self.assertEqual(origin,headers["Access-Control-Allow-Origin"])
    def test_access_control_not_allow_origin(self):
        self.fill_sytem_data()
        self.fill_data()
        origin="http://localhost:3000"
        result = self.httpHandler.query({"collection": "example", "queryId": "even"}, body="{\"even\": \"1\"}",headers={"origin": origin})
        self.assertEqual(result["statusCode"],200)
        body=json.loads(result["body"])
        self.assertEqual(len(body["data"]),10)
        headers=result["headers"]
        self.assertNotIn("Access-Control-Allow-Origin",headers)
    # def test_query_all(self):
    #     self.fillSystemData()
    #     self.fill_data()
    #     result = self.httpHandler.query({"collection": "example"}, body="{}",headers={"Origin": "http://localhost:3000"})
    #     self.assertEqual(result["statusCode"],200)
    #     body=json.loads(result["body"])
    #     self.assertEqual(len(body["data"]),20)

    def test_query_not_handled(self):
        self.fill_data()
        result = self.httpHandler.query({"collection": "example", "queryId": "whatever"}, body="{\"title\": \"data_1\"}",headers={"Origin": "http://localhost:3000"})
        self.assertEqual(result["statusCode"],400)

    def assertDictEqualsIgnoringFields(self, d1: dict, d2: dict, fields: List[str] = []):
        d1 = {k: v for k, v in d1.items() if k not in fields}
        d2 = {k: v for k, v in d2.items() if k not in fields}
        self.assertDictEqual(d1, d2)



if __name__ == '__main__':
    unittest.main()
