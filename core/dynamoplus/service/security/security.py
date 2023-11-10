import json
import uuid
from typing import *
from base64 import b64decode, b64encode
import os
import re
import logging
import time
import jwt

from dynamoplus.v2.service.system.system_service_v2 import AuthorizationService
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# from Crypto.PublicKey import RSA
# from Crypto.Signature import PKCS1_v1_5
# from Crypto.Hash import SHA256
import rsa
from dynamoplus.models.system.client_authorization.client_authorization import ScopesType, Scope, \
    ClientAuthorizationApiKey

JWT_SECRET = os.getenv("JWT_SECRET")

API_KEY_PREFIX = "dynamoplus-api-key"
CLIENT_ID_HEADER = "Dynamoplus-Client-Id"
BEARER_PREFIX = "Bearer"

path_regex = r"dynamoplus\/(\w+)(\/query)*(\/.*)*"


class SecurityService:

    @staticmethod
    def is_bearer(headers: dict):
        return SecurityService.is_authorization_of_type(headers, BEARER_PREFIX)

    @staticmethod
    def is_http_signature(headers: dict):
        return SecurityService.is_authorization_of_type(headers, 'signature')

    @staticmethod
    def is_api_key(headers: dict):
        return SecurityService.is_authorization_of_type(headers, API_KEY_PREFIX)

    @staticmethod
    def is_authorization_of_type(headers: dict, type: str):
        return SecurityService.get_authorization_value(headers, type) != None

    @staticmethod
    def get_authorization_value(headers: dict, type: str):
        headers = dict((k.lower(), v) for k, v in headers.items())
        if "authorization" in headers:
            authorization_header = headers['authorization']
            split = authorization_header.split(' ', 1)
            return split[1] if len(split) == 2 and split[0].strip().lower() == type.lower() else None

    @staticmethod
    def is_basic_auth(headers: dict):
        return SecurityService.is_authorization_of_type(headers, 'basic')

    @staticmethod
    def get_client_authorization_using_http_signature_authorized(headers: dict, method: str, path: str):
        headers = dict((k.lower(), v) for k, v in headers.items())
        if "authorization" in headers:
            authorization_header = headers["authorization"]
            if authorization_header.startswith("Signature"):
                authorization_value = authorization_header.replace("Signature", "")
                signature_components = {}
                if authorization_value:
                    for v in authorization_value.split(","):
                        key, value = v.split("=", 1)
                        signature_components[key.strip()] = value.replace("\"", "")
                    if "keyId" in signature_components:
                        key_id = signature_components["keyId"]
                        logging.info("client {}".format(key_id))
                        client_authorization = AuthorizationService.get_client_authorization(uuid.UUID(key_id))
                        if client_authorization:
                            signatory_message = "(request-target): {} {}".format(method, path)
                            for h in filter(lambda header: header.lower() != 'authorization', headers):
                                signatory_message += "\n{}: {}".format(h.lower(), headers[h])
                            signature = signature_components["signature"].replace("\"", "")
                            try:
                                public_key = rsa.PublicKey.load_pkcs1_openssl_pem(
                                    bytes(client_authorization.client_public_key, 'UTF-8'))

                                if rsa.verify(signatory_message.encode("utf-8"), b64decode(signature), public_key):
                                    return client_authorization
                                else:
                                    logging.error("signature not verified for key id {}".format(key_id))
                            except Exception as e:
                                logging.error("signature not verified for key id {} - {}".format(key_id, e))
                            # rsakey = RSA.importKey(client_authorization.client_public_key)
                            # signer = PKCS1_v1_5.new(rsakey)
                            # ## digest = SHA256.new()
                            #
                            # ## digest.update(b64decode(signatory_message))
                            # hash = SHA256.new(signatory_message.encode("utf-8"))
                            # if signer.verify(hash, b64decode(signature)):
                            #     return client_authorization
                            # else:
                            #     logging.error("signature not verified for key id {}".format(key_id))
                        else:
                            logging.error("client authorization not found for key {}".format(key_id))
                    else:
                        logging.error("missing key id")
                else:
                    logging.error("missing authorization value ")

    @staticmethod
    def get_basic_auth_authorized(headers: dict):
        authorization_value = SecurityService.get_authorization_value(headers, "basic")
        username, password = b64decode(authorization_value).decode().split(':', 1)
        if os.environ['ROOT_ACCOUNT'] == username and os.environ['ROOT_PASSWORD'] == password:
            return username

    @staticmethod
    def get_client_authorization_by_api_key(headers: dict, client_authentication_loader: Callable[[str, int], ClientAuthorizationApiKey] = None):
        authorization_value = SecurityService.get_authorization_value(headers, API_KEY_PREFIX)
        logger.info("authorization value = {}".format(authorization_value))
        if authorization_value and CLIENT_ID_HEADER in headers:
            client_id = headers[CLIENT_ID_HEADER]
            logger.info("client_id = {}".format(client_id))
            api_key = None
            if client_authentication_loader:
                results, next_id = client_authentication_loader(client_id, 1)
                client_authorization_api_key = results[0]
                api_key = client_authorization_api_key.api_key
                logger.info("found client authorization {}".format(api_key))
                if api_key == authorization_value:
                    return client_authorization_api_key
            else:
                client_authorization = AuthorizationService.get_client_authorization(uuid.UUID(client_id))
                api_key = client_authorization.api_key
                logger.info("found client authorization {}".format(client_authorization))
                if api_key == authorization_value:
                    return client_authorization

    @staticmethod
    def check_scope(path: str, method: str, client_scopes: List[Scope]):
        match = re.search(path_regex, path)
        if match:
            collection_name = match.group(1)
            is_query = len(match.groups()) >= 1 and match.group(2) == '/query'
            assigned_scope = None
            if method.lower() == 'post' and is_query:
                assigned_scope = ScopesType.QUERY
            elif method.lower() == 'post' and not is_query:
                assigned_scope = ScopesType.CREATE
            elif method.lower() == 'put':
                assigned_scope = ScopesType.UPDATE
            elif method.lower() == 'get':
                assigned_scope = ScopesType.GET
            elif method.lower() == 'delete':
                assigned_scope = ScopesType.DELETE

            for scope in filter(lambda cs: cs.collection_name == collection_name, client_scopes):
                if assigned_scope.name.lower() == scope.scope_type.name.lower():
                    return True
                else:
                    logger.error(
                        "method {} doesn't match the scope {}".format(method.lower(), scope.scope_type.name.lower()))
                # if assigned_scope == ScopesType.QUERY:
                #     if (scope.scope_type == ScopesType.CREATE and not is_query) or (
                #             scope.scope_type == ScopesType.QUERY and is_query):
                #         return True
                # elif method.lower() == scope.scope_type.name.lower():
                #     return True
                # else:
                #     logger.error("method {} doesn't match the scope {}".format(method.lower(),scope.scope_type.name.lower()))
        return False

    @staticmethod
    def get_jwt_token(username):
        jwt_payload = {"username": username, "expiration": int(time.time() * 1000.0) + (15 * 60 * 1000)}
        return jwt.encode(jwt_payload, JWT_SECRET, "HS256")

    @staticmethod
    def get_bearer_authorized(headers: dict):
        jwt_secret = JWT_SECRET if JWT_SECRET is not None else os.getenv("JWT_SECRET")
        authorization_value = SecurityService.get_authorization_value(headers, BEARER_PREFIX)
        payload = jwt.decode(authorization_value, jwt_secret, "HS256")
        # payload = json.loads(token_decoded)
        logging.debug("payload = {}".format(payload))
        expiration = payload["expiration"]
        if expiration > int(time.time() * 1000.0):
            return payload["username"]
