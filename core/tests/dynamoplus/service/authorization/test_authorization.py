import unittest
import os
import uuid
from unittest.mock import patch

import pytest

from dynamoplus.service.security.security import SecurityService
from dynamoplus.models.system.client_authorization.client_authorization import Scope, ScopesType, \
    ClientAuthorizationHttpSignature, ClientAuthorizationApiKey
from dynamoplus.v2.service.system.system_service_v2 import AuthorizationService

@pytest.mark.skip(reason="Authorization will be disabled")
class TestAuthorization(unittest.TestCase):



    def setUp(self) -> None:
        super().setUp()
        os.environ['ROOT_ACCOUNT'] = 'root'
        os.environ['ROOT_PASSWORD'] = 'password'
        os.environ['JWT_SECRET'] = '12345'
        self.public_key = "-----BEGIN PUBLIC KEY-----\n" \
                          "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDCFENGw33yGihy92pDjZQhl0C3\n" \
                          "6rPJj+CvfSC8+q28hxA161QFNUd13wuCTUcq0Qd2qsBe/2hFyc2DCJJg0h1L78+6\n" \
                          "Z4UMR7EOcpfdUE9Hf3m/hs+FUR45uBJeDK1HSFHD8bHKD6kv8FPGfJTotc+2xjJw\n" \
                          "oYi+1hqp1fIekaxsyQIDAQAB" \
                          "\n-----END PUBLIC KEY-----"
        self.client_authorization_http_signature = ClientAuthorizationHttpSignature(str(uuid.uuid4()),
                                                                                    [Scope("foo", "POST")],
                                                                                    self.public_key)
        self.client_authorization_api_key = ClientAuthorizationApiKey(str(uuid.uuid4()), [Scope("foo", ScopesType.CREATE)],
                                                                      "my-api-key", ["*"])

    def tearDown(self) -> None:
        super().tearDown()
        os.environ['ROOT_ACCOUNT'] = ''
        os.environ['ROOT_PASSWORD'] = ''
        os.environ['JWT_SECRET'] = ''

    def test_is_bearer(self):
        headers = {"Authorization": "Bearer xyz"}
        result = SecurityService.is_bearer(headers)
        self.assertEqual(True, result)

    def test_is_basic_auth(self):
        headers = {"Authorization": "Basic xyz"}
        result = SecurityService.is_basic_auth(headers)
        self.assertEqual(True, result)

    def test_is_not_basic_auth(self):
        headers = {"Authorization": "Signature xyz"}
        result = SecurityService.is_basic_auth(headers)
        self.assertEqual(False, result)

    def test_is_http_signature(self):
        headers = {"Authorization": "Signature xyz"}
        result = SecurityService.is_http_signature(headers)
        self.assertEqual(True, result)

    def test_is_not_http_signature(self):
        headers = {"Authorization": "Basic xyz"}
        result = SecurityService.is_http_signature(headers)
        self.assertEqual(False, result)

    def test_is_api_key(self):
        headers = {"Authorization": "dynamoplus-api-key xyz"}
        result = SecurityService.is_api_key(headers)
        self.assertEqual(True, result)

    def test_is_not_api_key(self):
        headers = {"Authorization": "Basic xyz"}
        result = SecurityService.is_api_key(headers)
        self.assertEqual(False, result)

    def test_is_authorized_basic_auth(self):
        headers = {"Authorization": "Basic cm9vdDpwYXNzd29yZA=="}
        result = SecurityService.get_basic_auth_authorized(headers)
        self.assertEqual("root", result)

    def test_is_not_authorized_basic_auth(self):
        headers = {"Authorization": "Basic YWRtaW46cGFzc3dvcmQ="}
        result = SecurityService.get_basic_auth_authorized(headers)
        self.assertIsNone(result)

    def test_is_authorized_bearer(self):
        headers = {
            "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6InJvb3QiLCJleHBpcmF0aW9uIjo3MjU4MTE4NDAwMDAwfQ.IFv74hy7vodW_jePgabrgR2GPRc5AsQGgH-bC-IrmQA"
        }
        result = SecurityService.get_bearer_authorized(headers)
        self.assertEqual("root", result)

    def test_is_not_authorized_bearer(self):
        headers = {
            "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6InJvb3QiLCJleHBpcmF0aW9uIjoxNTQ2MzAwODAwMDAwfQ.v-F13TfSZI-eEcTz6u2bGFzxY-EK8NePE2fZG5iCl9g"}
        result = SecurityService.get_bearer_authorized(headers)
        self.assertIsNone(result)

    @patch.object(AuthorizationService, "get_client_authorization")
    def test_get_signature(self, get_client_authorization):
        get_client_authorization.return_value = self.client_authorization_http_signature
        headers = {'Content-Type': 'application/json; charset=utf-8',
                   'Content-Length': '2',
                   'Host': 'localhost',
                   'Connection': 'Keep-Alive',
                   'Accept-Encoding': 'gzip',
                   'User-Agent': 'okhttp/3.0.0-RC1',
                   'Digest': 'SHA-256=RBNvo1WzZ4oRRq0W9+hknpT7T8If536DEMBg9hyq/4o=',
                   'Authorization': 'Signature keyId="client-id-category-readonly-1584047552748",algorithm="rsa-sha256",headers="(request-target) content-type content-length host connection accept-encoding user-agent digest",signature="fGn5ow1L5LalPHtSNs2B5hGz8R9TPSexE9p/ZtjgezeJYMuGzd8vPUeP248OX6mnDKqwMG/CEwM8gf16gY6y9BiDU8b1NLjqMpk8ekroO24hXDhePBo3WxSaxEG7v8EDWDRS0j9h3pMd0hZzHETcBcCJPK1hrKfyWnmNcgf1whw="'}

        path = "/dynamoplus/category/query"
        method = "post"
        client_authorization = SecurityService.get_client_authorization_using_http_signature_authorized(headers,
                                                                                                             method,
                                                                                                             path)
        self.assertIsNotNone(client_authorization)
        self.assertEqual(self.client_authorization_http_signature, client_authorization)

    @patch.object(AuthorizationService, "get_client_authorization")
    def test_get_client_authorization_http_signature(self, get_client_authorization):
        get_client_authorization.return_value = self.client_authorization_http_signature
        headers = {"Host": "example.com",
                   "Date": "Sun, 05 Jan 2014 21:31:40 GMT",
                   "Content-Type": "application/json",
                   "Digest": "SHA-256=X48E9qOokqqrvdts8nOJRJN3OWDUoyWxBf7kbu9DBPE=",
                   "Content-Length": "18",
                   "Authorization": "Signature keyId=\"my-client-id\",algorithm=\"rsa-sha256\",headers=\"(request-target) host date content-type digest content-length\", signature=\"vSdrb+dS3EceC9bcwHSo4MlyKS59iFIrhgYkz8+oVLEEzmYZZvRs8rgOp+63LEM3v+MFHB32NfpB2bEKBIvB1q52LaEUHFv120V01IL+TAD48XaERZFukWgHoBTLMhYS2Gb51gWxpeIq8knRmPnYePbF5MOkR0Zkly4zKH7s1dE=\""
                   }
        path = "/foo?param=value&pet=dog"
        method = "post"
        client_authorization = SecurityService.get_client_authorization_using_http_signature_authorized(headers,
                                                                                                             method,
                                                                                                             path)
        self.assertIsNotNone(client_authorization)
        self.assertEqual(self.client_authorization_http_signature, client_authorization)

    @patch.object(AuthorizationService, "get_client_authorization")
    def test_get_scopes_api_key(self, get_client_authorization):
        get_client_authorization.return_value = self.client_authorization_api_key
        headers = {"Authorization": "dynamoplus-api-key my-api-key", "dynamoplus-client-id": "my-client-id"}
        client_authorization = SecurityService.get_client_authorization_by_api_key(headers)
        self.assertEqual("my-client-id", client_authorization.client_id)
        self.assertEqual(1, len(client_authorization.client_scopes))
        self.assertEqual("foo", client_authorization.client_scopes[0].collection_name)
        self.assertEqual("foo", client_authorization.client_scopes[0].collection_name)
        self.assertEqual("CREATE", client_authorization.client_scopes[0].scope_type.name)

    def test_check_scope_authorized_create(self):
        client_scopes = [Scope("whatever", ScopesType.CREATE), Scope("example", ScopesType.CREATE)]
        result = SecurityService.check_scope("/dynamoplus/example", "POST", client_scopes)
        self.assertEqual(True, result)

    def test_check_scope_authorized_query(self):
        client_scopes = [Scope("whatever", ScopesType.CREATE), Scope("example", ScopesType.QUERY)]
        result = SecurityService.check_scope("/dynamoplus/example/query/by_key", "POST", client_scopes)
        self.assertEqual(True, result)

    def test_check_scope_authorized_query_all(self):
        client_scopes = [Scope("example", ScopesType.QUERY), Scope("example", ScopesType.GET)]
        result = SecurityService.check_scope("/dynamoplus/example/query", "POST", client_scopes)
        self.assertEqual(True, result)

    def test_check_scope_not_authorized_query(self):
        client_scopes = [Scope("whatever", ScopesType.CREATE), Scope("example", ScopesType.CREATE)]
        result = SecurityService.check_scope("/dynamoplus/example/query/by_key", "POST", client_scopes)
        self.assertEqual(False, result)
