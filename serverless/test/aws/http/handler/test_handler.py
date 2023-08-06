import json
import os
import unittest
import uuid
from datetime import datetime
from typing import *

import boto3
from moto import mock_dynamodb2

from aws.http.handler.handler_v2 import HttpHandler


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

    def fill_system_data(self):
        self._create_collection("example",
                                "id", [
                                    {"name": "field1", "type": "string"},
                                    {"name": "field2.field21", "type": "string"}
                                ])
        self.assert_collection_created()
        ## client authorization
        self._create_client_authorization_api_key("example-client-id",
                                                  "test-api-key",
                                                  [
                                                      {
                                                          "collection_name": "example",
                                                          "scope_type": "GET"
                                                      }
                                                  ])
        get_result = self.httpHandler.get(path_parameters={"collection": "client_authorization", "id": "example-client-id"})
        self._assertOkWithBody(get_result, {
            "type": "api_key",
            "client_id": "example-client-id",
            "api_key": "test-api-key",
            "client_scopes":[
                {
                    "collection_name": "example",
                    "scope_type": "GET"
                }
            ]
        })
        ## index 1 - field1__field2.field21
        self._create_index(index_name="example__field1__field2.field21", collection_name='example',
                           collection_id_key='id',
                           index_fields=[
                               {'field1': 'string'},
                               {'field2.field21': 'string'}
                           ])
        get_index_1_result = self.httpHandler.get(path_parameters={"collection": "index", "id": "example__field1__field2.field21"})
        self.assertHttpStatusCode(get_index_1_result, 200)
        self.assertDictEqual()
        ##index 2 - even
        self._create_index(index_name="example__even", collection_name="example", collection_id_key='id',
                           index_fields=[{"even": "boolean"}])

        ## index 3 - starting
        self._create_index(index_name="example__starting", collection_name="example", collection_id_key='id',
                           index_fields=[{"starting": "boolean"}])

    def assert_collection_created(self, collection_name="example", expected_id="id", expected_attributes=[
        {"name": "field1", "type": "STRING"},
        {"name": "field2.field21", "type": "STRING"},
    ]):
        get_collection = self.httpHandler.get(path_parameters={"collection": "collection", "id": collection_name})
        self.assertHttpStatusCode(get_collection, 200)
        self.assertDictEqualsIgnoringFields(json.loads(get_collection["body"]), {
            "name": collection_name,
            "id_key": expected_id,
            "attributes": expected_attributes,
            "auto_generate_id": False
        })

    def _create_index(self, index_name: str,
                      collection_name: str,
                      collection_id_key: str,
                      index_fields: [dict]):
        index_fields_names = [next(iter(d)) for d in index_fields]
        index_document = {
            'id': uuid.uuid4().hex,
            'name': index_name,
            'collection': {
                'id_key': collection_id_key,
                'name': collection_name
            },
            'conditions': index_fields_names
        }
        index_id = index_document['id']
        pk = ("index#%s" % index_id)
        self.insert_system_table(pk, "index",
                                 index_id,
                                 index_document)
        self.insert_system_table(pk,
                                 "index#collection.name",
                                 ("%s" % (collection_name)),
                                 index_document)
        self.insert_system_table(pk,
                                 "index#collection.name#fields",
                                 ("%s#%s" % (collection_name, '__'.join(index_fields_names))), index_document)

    def _create_client_authorization_api_key(self, client_id: uuid,
                                             api_key: str,
                                             client_scopes: [dict]):
        self.insert_system_table("client_authorization#%s" % str(client_id),
                                 "client_authorization",
                                 "%s" % str(client_id),
                                 {

                                     "type": "api_key",
                                     "client_id": client_id,
                                     "api_key": api_key,
                                     "client_scopes": client_scopes
                                 }
                                 )

    def _create_collection(self, collection_name: str, id_key: str, fields: [dict]):
        self.insert_system_table(("collection#%s" % collection_name), "collection", ("%s" % collection_name), {
            "id_key": id_key,
            "name": collection_name,
            "attributes": fields
        })

    def insert_system_table(self, pk: str,
                            sk: str,
                            data: str,
                            document: dict):
        self.systemTable.put_item(Item={"pk": pk,
                                        "sk": sk,
                                        "data": data,
                                        "document": document})

    def fill_data(self):
        timestamp = datetime.utcnow()
        document = {"name": "example", "id_key": "id", "ordering_key": "ordering",
                    "creation_date_time": timestamp.isoformat()}
        # starting 19/11/19
        target = 1574169491000 / 1000
        one_hour = 60 * 60
        for i in range(1, 21):
            starting = target + (i * one_hour * 24)
            ending = starting + one_hour
            document = {"id": str(i), "title": "data_" + str(i), "even": str(i % 2),
                        "starting": datetime.utcfromtimestamp(starting).isoformat(),
                        "ending": datetime.utcfromtimestamp(ending).isoformat(), "ordering": str(i)}
            self.table.put_item(
                Item={"pk": "example#" + str(i), "sk": "example", "data": str(i), "document": document})
            # self.insert_system_table("example#" + str(i), "example", ("%s"), document)
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#title", "data": "data_" + str(i),
                                      "document": document})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#even", "data": str(i % 2),
                                      "document": document})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#starting",
                                      "data": datetime.utcfromtimestamp(starting).isoformat(),
                                      "document": document})
            self.table.put_item(Item={"pk": "example#" + str(i), "sk": "example#ending",
                                      "data": datetime.utcfromtimestamp(ending).isoformat(),
                                      "document": document})

    @unittest.skip("not supported by moto")
    def test_update_client_authorization(self):
        ## given
        client_id = uuid.uuid4()
        self._create_client_authorization_api_key(client_id, "test-api-key-1",
                                                  [{"collection_name": "example", "scope_type": "GET"}])

        ## when
        path_parameters = {"collection": "client_authorization", "id": client_id}
        body = {"type": "api_key", "api_key": "test-api-key-2",
                "client_scopes": [{"collection_name": "example", "scope_type": "GET"}]}
        result = self.httpHandler.update(path_parameters=path_parameters, body=json.dumps(body))

        ## then
        self.assertEqual(result["statusCode"], 200)
        self.assertDictEqual(json.loads(result["body"]), {"client_id": client_id, **body})

    def test_delete_client_authorization(self):
        self._create_client_authorization_api_key("example-client-id",
                                                  "test-api-key",
                                                  [
                                                      {
                                                          "collection_name": "example",
                                                          "scope_type": "GET"
                                                      }
                                                  ])
        path_parameters = {"collection": "client_authorization", "id": "example-client-id"}
        body = {"type": "api_key", "client_id": "example-client-id", "api_key": "test-api-key-2",
                "client_scopes": [{"collection_name": "example", "scope_type": "GET"}]}
        result = self.httpHandler.delete(path_parameters=path_parameters)
        self.assertEqual(result["statusCode"], 200)

    def test_get_client_authorization(self):
        self._create_client_authorization_api_key("example-client-id",
                                                  "test-api-key",
                                                  [
                                                      {
                                                          "collection_name": "example",
                                                          "scope_type": "GET"
                                                      }
                                                  ])
        result = self.httpHandler.get(path="",
                                      path_parameters={"collection": "client_authorization", "id": "example-client-id"},
                                      query_string_parameters=[])
        self.assertEqual(result["statusCode"], 200)

    def test_getTargetEntity(self):
        path_parameters = {"collection": "example", "query": "name"}
        result = self.httpHandler.get_document_type_from_path_parameters(path_parameters)
        self.assertEqual(result, "example")

    def test_get_entityNotHandled(self):
        self._create_collection("example",
                                "id_key", [
                                    {"name": "field1", "type": "string"},
                                    {"name": "field2.field21", "type": "string"}
                                ])
        result = self.httpHandler.get({"collection": "whatever", "id": "1"})
        self.assertEqual(result["statusCode"], 400)

    def test_get_document_found(self):
        self._create_collection("example",
                                "id_key", [
                                    {"title": "string"}
                                ])
        document_id = "1"
        expected_result = {"id": ("%s" % document_id), "title": "data_1", "ordering": "1"}
        id = self._create_document("example", document_id, {"title": "data_1", "ordering": "1", "id": document_id})
        result = self.httpHandler.get({"collection": "example", "id": "1"})
        self.assertEqual(result["statusCode"], 200)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expected_result, ["even", "starting", "ending"])

    def _create_document(self, collection_name: str, document_id: str, document: dict):
        self.table.put_item(
            Item={"pk": collection_name + "#" + str(document_id), "sk": ("%s" % collection_name),
                  "data": str(document_id),
                  "document": document})
        return document_id

    def test_create_client_authorization(self):
        path_parameters = {"collection": "client_authorization"}
        body = {"type": "api_key", "client_id": "test", "api_key": "test-api-key",
                "client_scopes": [{"collection_name": "example", "scope_type": "GET"}]}
        create_result = self.httpHandler.create(path_parameters=path_parameters, body=json.dumps(body))
        self.assertHttpStatusCode(create_result)
        self.assertDictEqual(json.loads(create_result["body"]), body)
        self._assert_stored_in_system_table(body,
                                            "client_authorization#test",
                                            "client_authorization",
                                            "test")
        get_result = self.httpHandler.get(path_parameters={"collection": "client_authorization", "id": "test"})
        self._assertOkWithBody(get_result, json.loads(create_result["body"]))

    def test_create_collection(self):
        collection_name = "example"
        path_parameters = {"collection": "collection"}
        body = {
            "id_key": "id",
            "name": collection_name,
            "attributes": [
                {"name": "field1", "type": "STRING"},
                {"name": "field2.field21", "type": "STRING"}
            ],
            "auto_generate_id": False
        }
        create_result = self.httpHandler.create(path_parameters=path_parameters, body=json.dumps(body))
        self.assertHttpStatusCode(create_result)
        self.assertDictEqualsIgnoringFields(json.loads(create_result["body"]), body, "ordering_key")
        self._assert_stored_in_system_table({**body, "ordering": None},
                                            "collection#example",
                                            "collection",
                                            collection_name)
        get_result = self.httpHandler.get(path_parameters={"collection": "collection", "id": collection_name})
        self._assertOkWithBody(get_result, json.loads(create_result["body"]))

    def test_create_document(self):
        self._create_collection('example', 'id', [{"title": "string"}])
        expected_result = {"id": "1000", "title": "test_1", "ordering": "21"}
        result = self.httpHandler.create({"collection": "example"},
                                         body="{\"id\":\"1000\", \"title\": \"test_1\",\"ordering\": \"21\"}")
        self.assertHttpStatusCode(result, 201)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expected_result,
                                            ["id", "creation_date_time", "order_unique"])

    def assertHttpStatusCode(self, result, expected_status_code=201):
        self.assertEqual(result["statusCode"], expected_status_code, result["body"])

    @unittest.skip("not supported by moto")
    def test_update_adding_new_field(self):
        self.fill_system_data()
        self.fill_data()
        expected_result = {"id": "1", "title": "test_1", "ordering": "21", "new_attribute": "001"}
        result = self.httpHandler.update({"collection": "example", "id": "1"}, body="{\"title\": \"test_1\", "
                                                                                    "\"new_attribute\": \"001\", \"ordering\": "
                                                                                    "\"21\"}")
        self.assertEqual(result["statusCode"], 200)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expected_result,
                                            ["creation_date_time", "update_date_time"])

    @unittest.skip("not supported by moto")
    def test_update_edit_existing_field(self):
        self.fill_system_data()
        self.fill_data()
        expected_result = {"id": "1", "title": "test_1", "ordering": "21"}
        result = self.httpHandler.update({"collection": "example"},
                                         body="{\"id\":\"1\", \"title\": \"test_1\", \"ordering\": \"21\"}")
        self.assertEqual(result["statusCode"], 200)
        self.assertDictEqualsIgnoringFields(json.loads(result["body"]), expected_result,
                                            ["creation_date_time", "update_date_time"])

    def test_delete_document(self):
        self._create_collection("example",
                                "id_key", [
                                    {"title": "string"}
                                ])
        document_id = "1"
        id = self._create_document("example", document_id, {"title": "data_1", "ordering": "1", "id": document_id})
        result = self.httpHandler.delete({"collection": "example", "id": id})
        self.assertEqual(result["statusCode"], 200)

    def test_query(self):
        self.fill_system_data()
        self.fill_data()
        origin = "http://localhost"
        request_body = json.dumps({
            "matches": {
                "eq": {"field_name": "even", "value": "1"}
            }
        })
        result = self.httpHandler.query({"collection": "example"}, body=request_body, headers={"origin": origin})
        self.assertEqual(result["statusCode"], 200)
        body = json.loads(result["body"])
        self.assertEqual(len(body["data"]), 10)
        headers = result["headers"]
        self.assertIn("Access-Control-Allow-Origin", headers)
        self.assertEqual(origin, headers["Access-Control-Allow-Origin"])

    def test_query_by_range(self):
        self.fill_system_data()
        self.fill_data()
        origin = "http://localhost"
        starting = 1574428691000 / 1000
        one_hour = 60 * 60
        request_body = json.dumps({"matches": {
            "range": {
                "field_name": "starting",
                "from": datetime.utcfromtimestamp(starting).isoformat()[:-6],
                "to": datetime.utcfromtimestamp(starting + (one_hour * 25)).isoformat()[:-6]
            }
        }})
        result = self.httpHandler.query({"collection": "example"}, body=request_body,
                                        headers={"origin": origin})
        self.assertEqual(result["statusCode"], 200)
        body = json.loads(result["body"])
        self.assertEqual(len(body["data"]), 2)
        headers = result["headers"]
        self.assertIn("Access-Control-Allow-Origin", headers)
        self.assertEqual(origin, headers["Access-Control-Allow-Origin"])

    def test_query_with_limit(self):
        self.fill_system_data()
        self.fill_data()
        origin = "http://localhost"
        request_body = json.dumps({
            "matches": {"eq": {"field_name": "even", "value": "1"}}
        })
        result = self.httpHandler.query({"collection": "example"},
                                        query_string_parameters={"limit": "2"},
                                        body=request_body,
                                        headers={"origin": origin})
        self.assertEqual(result["statusCode"], 200)
        body = json.loads(result["body"])
        self.assertEqual(len(body["data"]), 2)
        self.assertIn("has_more", body)
        self.assertEqual(True, body["has_more"])
        headers = result["headers"]
        self.assertIn("Access-Control-Allow-Origin", headers)
        self.assertEqual(origin, headers["Access-Control-Allow-Origin"])

    def test_query_with_pagination(self):
        self.fill_system_data()
        self.fill_data()
        origin = "http://localhost"
        request_body = json.dumps({
            "matches": {"eq": {"field_name": "even", "value": "1"}}
        })
        result = self.httpHandler.query({"collection": "example"},
                                        query_string_parameters={"limit": "2", "start_from": "3"},
                                        body=request_body,
                                        headers={"origin": origin})
        self.assertEqual(result["statusCode"], 200)
        body = json.loads(result["body"])
        self.assertEqual(len(body["data"]), 1)
        self.assertIn("has_more", body)
        self.assertEqual(False, body["has_more"])
        headers = result["headers"]
        self.assertIn("Access-Control-Allow-Origin", headers)
        self.assertEqual(origin, headers["Access-Control-Allow-Origin"])

    def test_access_control_allow_origin(self):
        self.fill_system_data()
        self.fill_data()
        origin = "http://localhost"
        request_body = json.dumps({
            "matches": {"eq": {"field_name": "even", "value": "1"}}
        })
        result = self.httpHandler.query({"collection": "example"},
                                        body=request_body,
                                        headers={"origin": origin})
        self.assertEqual(result["statusCode"], 200, result["body"])
        body = json.loads(result["body"])
        self.assertEqual(len(body["data"]), 10)
        headers = result["headers"]
        self.assertIn("Access-Control-Allow-Origin", headers)
        self.assertEqual(origin, headers["Access-Control-Allow-Origin"])

    def test_access_control_not_allow_origin(self):
        self.fill_system_data()
        self.fill_data()
        origin = "http://localhost:3000"
        request_body = json.dumps({
            "matches": {"eq": {"field_name": "even", "value": "1"}}
        })
        result = self.httpHandler.query({"collection": "example", "queryId": "even"},
                                        body=request_body, headers={"origin": origin})
        self.assertEqual(result["statusCode"], 200)
        body = json.loads(result["body"])
        self.assertEqual(len(body["data"]), 10)
        headers = result["headers"]
        self.assertNotIn("Access-Control-Allow-Origin", headers)

    # def test_query_all(self):
    #     self.fillSystemData()
    #     self.fill_data()
    #     result = self.httpHandler.query({"collection": "example"}, body="{}",headers={"Origin": "http://localhost:3000"})
    #     self.assertEqual(result["statusCode"],200)
    #     body=json.loads(result["body"])
    #     self.assertEqual(len(body["data"]),20)

    def test_query_not_handled(self):
        self.fill_data()
        result = self.httpHandler.query({"collection": "example", "queryId": "whatever"},
                                        body="{\"query\":{\"title\": \"data_1\"}}",
                                        headers={"Origin": "http://localhost:3000"})
        self.assertEqual(result["statusCode"], 400)

    def assertDictEqualsIgnoringFields(self, d1: dict, d2: dict, fields: List[str] = []):
        d1 = {k: v for k, v in d1.items() if k not in fields}
        d2 = {k: v for k, v in d2.items() if k not in fields}
        self.assertDictEqual(d1, d2)

    def _assertOkWithBody(self, result: dict, expected_body: dict):
        self.assertEqual(result["statusCode"], 200)
        self.assertDictEqual(json.loads(result["body"]), expected_body)

    def _assert_stored_in_system_table(self, expected_document: dict,
                                       expected_pk: str,
                                       expected_sk: str,
                                       expected_data: str):
        client_authorization_created = self.systemTable.get_item(
            Key={"pk": expected_pk, "sk": expected_sk})
        self.assertEqual(client_authorization_created["Item"]["data"], expected_data)
        self.assertDictEqual(client_authorization_created["Item"]["document"], expected_document)


def assert_dicts_equal_except_keys(dict1, dict2, ignore_keys):
    """
    Asserts that two dictionaries are equal except for specific keys.
    """
    assert set(dict1.keys()) == set(dict2.keys()), "Dictionaries have different keys"

    for key in dict1.keys():
        if key in ignore_keys:
            continue
        assert dict1[key] == dict2[key], f"Values for key '{key}' differ"


if __name__ == '__main__':
    unittest.main()
