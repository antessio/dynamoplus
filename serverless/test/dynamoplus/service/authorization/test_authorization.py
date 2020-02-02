import unittest
import os
from unittest.mock import patch
from dynamoplus.service.system.system import SystemService
from dynamoplus.service.authorization.authorization import AuthorizationService
from dynamoplus.models.system.client_authorization.client_authorization import  Scope,ScopesType,ClientAuthorization, ClientAuthorizationHttpSignature,ClientAuthorizationApiKey


class TestAuthorization(unittest.TestCase):

    def setUp(self) -> None:
        super().setUp()
        os.environ['ROOT_ACCOUNT'] = 'root'
        os.environ['ROOT_PASSWORD'] = 'password'
        self.public_key = "-----BEGIN PUBLIC KEY-----\n" \
                          "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDCFENGw33yGihy92pDjZQhl0C3" \
                          "6rPJj+CvfSC8+q28hxA161QFNUd13wuCTUcq0Qd2qsBe/2hFyc2DCJJg0h1L78+6" \
                          "Z4UMR7EOcpfdUE9Hf3m/hs+FUR45uBJeDK1HSFHD8bHKD6kv8FPGfJTotc+2xjJw" \
                          "oYi+1hqp1fIekaxsyQIDAQAB" \
                          "\n-----END PUBLIC KEY-----"
        self.client_authorization_http_signature = ClientAuthorizationHttpSignature("my-client-id",[Scope("foo","POST")],self.public_key)
        self.client_authorization_api_key = ClientAuthorizationApiKey("my-client-id", [Scope("foo",ScopesType.CREATE)],"my-api-key", ["*"])

    def tearDown(self) -> None:
        super().tearDown()
        os.environ['ROOT_ACCOUNT'] = ''
        os.environ['ROOT_PASSWORD'] = ''

    def test_is_basic_auth(self):
        headers = {"Authorization": "Basic xyz"}
        result = AuthorizationService.is_basic_auth(headers)
        self.assertEqual(True, result)

    def test_is_not_basic_auth(self):
        headers = {"Authorization": "Signature xyz"}
        result = AuthorizationService.is_basic_auth(headers)
        self.assertEqual(False, result)

    def test_is_http_signature(self):
        headers = {"Authorization": "Signature xyz"}
        result = AuthorizationService.is_http_signature(headers)
        self.assertEqual(True, result)

    def test_is_not_http_signature(self):
        headers = {"Authorization": "Basic xyz"}
        result = AuthorizationService.is_http_signature(headers)
        self.assertEqual(False, result)

    def test_is_api_key(self):
        headers = {"Authorization": "dynamoplus-api-key xyz"}
        result = AuthorizationService.is_api_key(headers)
        self.assertEqual(True, result)

    def test_is_not_api_key(self):
        headers = {"Authorization": "Basic xyz"}
        result = AuthorizationService.is_api_key(headers)
        self.assertEqual(False, result)

    def test_is_authorized_basic_auth(self):
        headers = {"Authorization": "Basic cm9vdDpwYXNzd29yZA=="}
        result = AuthorizationService.get_basic_auth_authorized(headers)
        self.assertEqual("root", result)

    def test_is_not_authorized_basic_auth(self):
        headers = {"Authorization": "Basic YWRtaW46cGFzc3dvcmQ="}
        result = AuthorizationService.get_basic_auth_authorized(headers)
        self.assertIsNone(result)

    @patch.object(SystemService, "get_client_authorization")
    def test_get_client_authorization_http_signature(self,get_client_authorization):
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
        client_authorization = AuthorizationService.get_client_authorization_using_http_signature_authorized(headers, method, path)
        self.assertIsNotNone(client_authorization)
        self.assertEqual(self.client_authorization_http_signature,client_authorization)

    @patch.object(SystemService, "get_client_authorization")
    def test_get_scopes_api_key(self,get_client_authorization):
        get_client_authorization.return_value = self.client_authorization_api_key
        headers = {"Authorization": "dynamoplus-api-key my-api-key", "dynamoplus-client-id": "my-client-id"}
        client_authorization = AuthorizationService.get_client_authorization_by_api_key(headers)
        self.assertEqual("my-client-id",client_authorization.client_id)
        self.assertEqual(1, len(client_authorization.client_scopes))
        self.assertEqual("foo", client_authorization.client_scopes[0].collection_name)
        self.assertEqual("foo", client_authorization.client_scopes[0].collection_name)
        self.assertEqual("CREATE", client_authorization.client_scopes[0].scope_type.name)

    def test_check_scope_authorized_create(self):
        client_scopes = [Scope("whatever", ScopesType.CREATE),Scope("example", ScopesType.CREATE)]
        result = AuthorizationService.check_scope("/dynamoplus/example","POST",client_scopes)
        self.assertEqual(True, result)

    def test_check_scope_authorized_query(self):
        client_scopes = [Scope("whatever", ScopesType.CREATE),Scope("example", ScopesType.QUERY)]
        result = AuthorizationService.check_scope("/dynamoplus/example/query/by_key","POST",client_scopes)
        self.assertEqual(True, result)

    def test_check_scope_not_authorized_query(self):
        client_scopes = [Scope("whatever", ScopesType.CREATE),Scope("example", ScopesType.CREATE)]
        result = AuthorizationService.check_scope("/dynamoplus/example/query/by_key","POST",client_scopes)
        self.assertEqual(False, result)