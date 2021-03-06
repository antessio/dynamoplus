from typing import *
from base64 import b64decode, b64encode
import os
import re
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA256
from dynamoplus.service.system.system import SystemService
from dynamoplus.models.system.client_authorization.client_authorization import ScopesType, Scope

API_KEY_PREFIX = "dynamoplus-api-key"
CLIENT_ID_HEADER = "dynamoplus-client-id"

path_regex = r"dynamoplus\/(\w+)(\/query)*(\/.*)*"


class AuthorizationService:

    @staticmethod
    def is_http_signature(headers: dict):
        return AuthorizationService.is_authorization_of_type(headers, 'signature')

    @staticmethod
    def is_api_key(headers: dict):
        return AuthorizationService.is_authorization_of_type(headers, API_KEY_PREFIX)

    @staticmethod
    def is_authorization_of_type(headers: dict, type: str):
        return AuthorizationService.get_authorization_value(headers, type) != None

    @staticmethod
    def get_authorization_value(headers: dict, type: str):
        headers = dict((k.lower(), v) for k, v in headers.items())
        if "authorization" in headers:
            authorization_header = headers['authorization']
            split = authorization_header.split(' ', 1)
            return split[1] if len(split) == 2 and split[0].strip().lower() == type.lower() else None

    @staticmethod
    def is_basic_auth(headers: dict):
        return AuthorizationService.is_authorization_of_type(headers, 'basic')

    @staticmethod
    def get_client_authorization_using_http_signature_authorized(headers: dict, method: str, path: str):
        headers = dict((k.lower(), v) for k, v in headers.items())
        if "authorization" in headers:
            authorization_header = headers["authorization"]
            if authorization_header.startswith("Signature"):
                authorization_value = authorization_header.replace("Signature","")
                signature_components = {}
                if authorization_value:
                    for v in authorization_value.split(","):
                        key,value = v.split("=",1)
                        signature_components[key.strip()]=value.replace("\"","")
                    if "keyId" in signature_components:
                        key_id = signature_components["keyId"]
                        logging.info("client {}".format(key_id))
                        client_authorization = SystemService.get_client_authorization(key_id)
                        if client_authorization:
                            signatory_message = "(request-target): {} {}".format(method,path);
                            for h in filter(lambda header: header.lower() != 'authorization', headers):
                                signatory_message += "\n{}: {}".format(h.lower(),headers[h])

                            rsakey = RSA.importKey(client_authorization.client_public_key)
                            signer = PKCS1_v1_5.new(rsakey)
                            ## digest = SHA256.new()
                            signature = signature_components["signature"].replace("\"", "")
                            ## digest.update(b64decode(signatory_message))
                            hash = SHA256.new(signatory_message.encode("utf-8"))
                            if signer.verify(hash, b64decode(signature)):
                                return client_authorization
                            else:
                                logging.error("signature not verified for key id {}".format(key_id))
                        else:
                            logging.error("client authorization not found for key {}".format(key_id))
                    else:
                        logging.error("missing key id")
                else:
                    logging.error("missing authorization value ")


    @staticmethod
    def get_basic_auth_authorized(headers: dict):
        authorization_value = AuthorizationService.get_authorization_value(headers, "basic")
        username, password = b64decode(authorization_value).decode().split(':', 1)
        if os.environ['ROOT_ACCOUNT'] == username and os.environ['ROOT_PASSWORD'] == password:
            return username

    @staticmethod
    def get_client_authorization_by_api_key(headers: dict):
        authorization_value = AuthorizationService.get_authorization_value(headers, API_KEY_PREFIX)
        logger.info("authorization value = {}".format(authorization_value))
        if authorization_value and CLIENT_ID_HEADER in headers:
            client_id = headers[CLIENT_ID_HEADER]
            logger.info("client_id = {}".format(client_id))
            client_authorization = SystemService.get_client_authorization(client_id)
            logger.info("found client authorization {}".format(client_authorization))
            if client_authorization.api_key == authorization_value:
                return client_authorization

    @staticmethod
    def check_scope(path: str, method: str, client_scopes: List[Scope]):
        match = re.search(path_regex, path)
        if match:
            collection_name = match.group(1)
            is_query = len(match.groups()) >= 1 and match.group(2) == '/query'
            for scope in filter(lambda cs: cs.collection_name == collection_name, client_scopes):
                if method.lower() == 'post':
                    if (scope.scope_type == ScopesType.CREATE and not is_query) or (scope.scope_type == ScopesType.QUERY and is_query):
                        return True
                elif method.lower() == scope.scope_type.name.lower():
                    return True
        return False
