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
        os.environ["STAGE"] = "local"
        os.environ["TEST_FLAG"] = "true"
        os.environ["ALLOWED_ORIGINS"] = "http://localhost"
        os.environ["DYNAMODB_DOMAIN_TABLE"] = "example-domain"
        os.environ["DYNAMODB_SYSTEM_TABLE"] = "example-system"
        self.dynamodb = boto3.resource("dynamodb", region_name='eu-west-1')
        os.environ["ENTITIES"] = "collection,index,client_authorization"
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
        ## client authorization
        self.systemTable.put_item(Item={"pk": "client_authorization#example-client-id","sk":"client_authorization", "data":"example-client-id",
                                        "document":"{\"type\":\"api_key\",\"client_id\":\"example-client-id\",\"api_key\":\"test-api-key\",\"client_scopes\":[{\"collection_name\":\"example\",\"scope_type\":\"GET\"}]}"})
        ## index 1 - field1__field2.field21
        self.systemTable.put_item(Item={"pk": "index#1", "sk": "index", "data": "1",
                                        "document": "{\"uid\": \"1\",\"name\":\"collection.name\",\"collection\":{\"id_key\":\"id\",\"name\":\"example\"},\"fields\": [{\"field1\": \"string\"}, {\"field2.field21\": \"string\"}]}"})
        self.systemTable.put_item(Item={"pk": "index#1", "sk": "index#collection.name", "data": "example",
                                        "document": "{\"uid\": \"1\",\"name\":\"collection.name\",\"collection\":{\"id_key\":\"id\",\"name\":\"example\"},\"fields\": [{\"field1\": \"string\"}, {\"field2.field21\": \"string\"}]}"})
        self.systemTable.put_item(Item={"pk": "index#1", "sk": "index#collection.name#name", "data": "example#field",
                                        "document": "{\"uid\": \"1\",\"name\":\"collection.name\",\"collection\":{\"id_key\":\"id\",\"name\":\"example\"},\"conditions\": [\"field1\",\"field2.field21\"],\"fields\": [{\"field1\": \"string\"}, {\"field2.field21\": \"string\"}]}"})

        ##index 2 - even
        self.systemTable.put_item(Item={"pk": "index#2", "sk": "index", "data": "2",
                                        "document": "{\"uid\": \"2\",\"name\":\"even\",\"collection\":{\"id_key\":\"id\",\"name\":\"example\"},\"conditions\": [\"even\"]}"})
        self.systemTable.put_item(Item={"pk": "index#2", "sk": "index#collection.name", "data": "example",
                                        "document": "{\"uid\": \"2\",\"name\":\"even\",\"collection\":{\"id_key\":\"id\",\"name\":\"example\"},\"conditions\": [\"even\"]}"})
        self.systemTable.put_item(Item={"pk": "index#2", "sk": "index#collection.name#name", "data": "example#even",
                                        "document": "{\"uid\": \"2\",\"name\":\"even\",\"collection\":{\"id_key\":\"id\",\"name\":\"example\"},\"conditions\": [\"even\"]}"})
        ## index 3 - starting
        self.systemTable.put_item(Item={"pk": "index#3", "sk": "index", "data": "3",
                                        "document": "{\"uid\": \"3\",\"name\":\"starting\",\"collection\":{\"id_key\":\"id\",\"name\":\"example\"},\"conditions\": [\"starting\",\"starting\"]}"})
        self.systemTable.put_item(Item={"pk": "index#3", "sk": "index#collection.name", "data": "example",
                                        "document": "{\"uid\": \"3\",\"name\":\"starting\",\"collection\":{\"id_key\":\"id\",\"name\":\"example\"},\"conditions\": [\"starting\",\"starting\"]}"})
        self.systemTable.put_item(Item={"pk": "index#3", "sk": "index#collection.name#name", "data": "example#starting",
                                        "document": "{\"uid\": \"3\",\"name\":\"starting\",\"collection\":{\"id_key\":\"id\",\"name\":\"example\"},\"conditions\": [\"starting\",\"starting\"]}"})


    def fill_data(self):
        timestamp = datetime.utcnow()
        document = {"name": "example", "id_key": "id", "ordering_key": "ordering",
                    "creation_date_time": timestamp.isoformat()}
        # starting 19/11/19
        target = 1574169491000/1000
        one_hour = 60*60
        for i in range(1, 21):
            starting = target+(i*one_hour*24)
            ending = starting+one_hour
            document = {"id": str(i), "title": "data_" + str(i), "even": str(i % 2), "starting": datetime.utcfromtimestamp(starting).isoformat(), "ending": datetime.utcfromtimestamp(ending).isoformat(),"ordering": str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#title", "data": "data_" + str(i),
                                      "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#even", "data": str(i % 2),
                                      "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#starting", "data": datetime.utcfromtimestamp(starting).isoformat(),
                                      "document": json.dumps(document)})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#ending", "data": datetime.utcfromtimestamp(ending).isoformat(),
                                      "document": json.dumps(document)})

    def test_update_client_authorization(self):
        path_parameters = {"collection": "client_authorization"}
        body = {"type":"api_key","client_id":"example-client-id","api_key":"test-api-key-2","client_scopes":[{"collection_name":"example","scope_type":"GET"}]}
        result = self.httpHandler.update(path_parameters=path_parameters,body=json.dumps(body))
        self.assertEqual(result["statusCode"],200)
        self.assertDictEqual(json.loads(result["body"]), body)

    def test_delete_client_authorization(self):
        path_parameters = {"collection": "client_authorization", "id":"example-client-id"}
        body = {"type":"api_key","client_id":"example-client-id","api_key":"test-api-key-2","client_scopes":[{"collection_name":"example","scope_type":"GET"}]}
        result = self.httpHandler.delete(path_parameters=path_parameters)
        self.assertEqual(result["statusCode"],200)

    def test_create_client_authorization(self):
        path_parameters = {"collection": "client_authorization"}
        body = {"type":"api_key","client_id":"test","api_key":"test-api-key","client_scopes":[{"collection_name":"example","scope_type":"GET"}]}
        result = self.httpHandler.create(path_parameters=path_parameters,body=json.dumps(body))
        self.assertEqual(result["statusCode"],201)
        self.assertDictEqual(json.loads(result["body"]), body)

    def test_get_client_authorization(self):
        self.fill_sytem_data()
        self.fill_data()
        result = self.httpHandler.get(path_parameters={"collection":"client_authorization","id":"example-client-id"},query_string_parameters=[])
        self.assertEqual(result["statusCode"],200)

    def test_getTargetEntity(self):
        path_parameters = {"collection": "example", "query": "name"}
        result = self.httpHandler.get_document_type_from_path_parameters(path_parameters)
        self.assertEqual(result, "example")

    def test_get_entityNotHandled(self):
        self.fill_sytem_data()
        self.fill_data()
        result = self.httpHandler.get({"collection":"whatever", "id":"1"})
        self.assertEqual(result["statusCode"],400)


    def test_get_found(self):
        self.fill_sytem_data()
        self.fill_data()
        expected_result = {"id": "1", "title": "data_1", "ordering": "1"}
        result = self.httpHandler.get({"collection": "example", "id": "1"})
        self.assertEqual(result["statusCode"], 200)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expected_result, ["even","starting","ending"])

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
        result = self.httpHandler.query({"collection": "example", "queryId": "even"}, body="{\"matches\":{\"even\": \"1\"}}",headers={"origin": origin})
        self.assertEqual(result["statusCode"],200)
        body=json.loads(result["body"])
        self.assertEqual(len(body["data"]),10)
        headers=result["headers"]
        self.assertIn("Access-Control-Allow-Origin",headers)
        self.assertEqual(origin,headers["Access-Control-Allow-Origin"])

    def test_query_by_range(self):
        self.fill_sytem_data()
        self.fill_data()
        origin = "http://localhost"
        starting = 1574428691000/1000
        one_hour = 60*60

        example = json.dumps({"matches":{"starting": [datetime.utcfromtimestamp(starting).isoformat()[:-6], datetime.utcfromtimestamp(starting + (one_hour * 25)).isoformat()[:-6]]}})
        result = self.httpHandler.query({"collection": "example", "queryId": "starting"}, body=example,
                                        headers={"origin": origin})
        self.assertEqual(result["statusCode"], 200)
        body = json.loads(result["body"])
        self.assertEqual(len(body["data"]), 2)
        headers = result["headers"]
        self.assertIn("Access-Control-Allow-Origin", headers)
        self.assertEqual(origin, headers["Access-Control-Allow-Origin"])

    def test_query_with_limit(self):
        self.fill_sytem_data()
        self.fill_data()
        origin = "http://localhost"
        result = self.httpHandler.query({"collection": "example", "queryId": "even"}, query_string_parameters={}, body="{\"matches\":{\"even\": \"1\"},\"limit\": 2}",
                                        headers={"origin": origin})
        self.assertEqual(result["statusCode"], 200)
        body = json.loads(result["body"])
        self.assertEqual(len(body["data"]), 2)
        self.assertIn("last_key",body)
        self.assertEqual("eyJwayI6ICJleGFtcGxlIzMiLCAic2siOiAiZXhhbXBsZSNldmVuIn0=",body["last_key"])
        headers = result["headers"]
        self.assertIn("Access-Control-Allow-Origin", headers)
        self.assertEqual(origin, headers["Access-Control-Allow-Origin"])

    def test_query_with_pagination(self):
        self.fill_sytem_data()
        self.fill_data()
        origin = "http://localhost"
        result = self.httpHandler.query({"collection": "example", "queryId": "even"}, query_string_parameters={}, body="{\"matches\":{\"even\": \"1\"},\"limit\": 2,\"last_key\": \"eyJwayI6ICJleGFtcGxlIzciLCAic2siOiAiZXhhbXBsZSNldmVuIn0=\" }",
                                        headers={"origin": origin})
        self.assertEqual(result["statusCode"], 200)
        body = json.loads(result["body"])
        self.assertEqual(len(body["data"]), 2)
        self.assertIn("last_key",body)
        self.assertEqual("eyJwayI6ICJleGFtcGxlIzMiLCAic2siOiAiZXhhbXBsZSNldmVuIn0=", body["last_key"])
        headers = result["headers"]
        self.assertIn("Access-Control-Allow-Origin", headers)
        self.assertEqual(origin, headers["Access-Control-Allow-Origin"])

    def test_access_control_allow_origin(self):
        self.fill_sytem_data()
        self.fill_data()
        origin="http://localhost"
        result = self.httpHandler.query({"collection": "example", "queryId": "even"}, body="{\"matches\":{\"even\": \"1\"}}",headers={"origin": origin})
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
        result = self.httpHandler.query({"collection": "example", "queryId": "even"}, body="{\"matches\":{\"even\": \"1\"}}",headers={"origin": origin})
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
        result = self.httpHandler.query({"collection": "example", "queryId": "whatever"}, body="{\"query\":{\"title\": \"data_1\"}}",headers={"Origin": "http://localhost:3000"})
        self.assertEqual(result["statusCode"],400)

    def assertDictEqualsIgnoringFields(self, d1: dict, d2: dict, fields: List[str] = []):
        d1 = {k: v for k, v in d1.items() if k not in fields}
        d2 = {k: v for k, v in d2.items() if k not in fields}
        self.assertDictEqual(d1, d2)



if __name__ == '__main__':
    unittest.main()
