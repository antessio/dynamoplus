import os
import logging

import json
from dynamoplus.service.authorization.authorization import AuthorizationService

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

AUTHORIZATION_FEATURE_ENABLED = True  # bool(os.getenvb("AUTHORIZATION_FEATURE_ENABLED","False"))
ROOT_ACCOUNT = os.getenv("ROOT_ACCOUNT")
ROOT_PASSWORD = os.getenv("ROOT_PASSWORD")


def issue_jwt(event, context):
    headers = dict((k.lower(), v) for k, v in event["headers"].items())
    try:
        if AuthorizationService.is_basic_auth(headers):
            username = AuthorizationService.get_basic_auth_authorized(headers)
            logging.info("Found {} in credentials".format(username))
            ## token expires in 15 minutes = 15*60*1000 ms
            token = AuthorizationService.get_jwt_token(username)
            return {"statusCode": 200, "body": json.dumps({"token": token.decode("utf-8") })}
        else:
            return {"statusCode": 401}
    except Exception as e:
        print(f'Exception encountered: {e}')
        logging.error("exception encountered", e)
        return {"statusCode": 500}


def authorize(event, context):
    headers = dict((k.lower(), v) for k, v in event["headers"].items())
    http_method = event["httpMethod"]
    path = event["path"]
    method_arn = event["methodArn"]
    policy = generate_policy("anonymous_client", "Deny", "*")
    if AUTHORIZATION_FEATURE_ENABLED:
        logging.debug("headers = {}".format(headers))
        try:
            if AuthorizationService.is_bearer(headers):
                username = AuthorizationService.get_bearer_authorized(headers)
                logging.info("Found {} in credentials".format(username))
                if username:
                    policy = generate_policy(username, "Allow", "*")
            elif AuthorizationService.is_basic_auth(headers):
                username = AuthorizationService.get_basic_auth_authorized(headers)
                logging.info("Found {} in credentials".format(username))
                if username:
                    policy = generate_policy(username, "Allow", "*")
            elif AuthorizationService.is_api_key(headers):
                client_authorization = AuthorizationService.get_client_authorization_by_api_key(headers)
                logger.info("client authorization = {}".format(client_authorization))
                if client_authorization and AuthorizationService.check_scope(path, http_method,
                                                                             client_authorization.client_scopes):
                    policy = generate_policy(client_authorization.client_id, "Allow", "*")
            elif AuthorizationService.is_http_signature(headers):
                client_authorization = AuthorizationService.get_client_authorization_using_http_signature_authorized(
                    headers, http_method.lower(), path)
                if client_authorization and AuthorizationService.check_scope(path, http_method,
                                                                             client_authorization.client_scopes):
                    policy = generate_policy(client_authorization.client_id, "Allow", "*")
        except Exception as e:
            print(f'Exception encountered: {e}')
            logging.error("exception encountered", e)
            policy = generate_policy("anonymous_client", "Deny", "*")
    else:
        policy = generate_policy("anonymous_client", "Allow", "*")

    return policy


def generate_policy(principal_id, effect, resource):
    return {
        'principalId': principal_id,
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': [
                {
                    "Action": "execute-api:Invoke",
                    "Effect": effect,
                    "Resource": resource

                }
            ]
        }
    }
