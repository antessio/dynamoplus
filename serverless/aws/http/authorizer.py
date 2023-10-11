try:
    import unzip_requirements
except ImportError:
    pass
import os
import logging
import json
from dynamoplus.service.security import SecurityService

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

AUTHORIZATION_FEATURE_ENABLED = True  # bool(os.getenvb("AUTHORIZATION_FEATURE_ENABLED","False"))
ROOT_ACCOUNT = os.getenv("ROOT_ACCOUNT")
ROOT_PASSWORD = os.getenv("ROOT_PASSWORD")


def check_allowed_origin(origin):
    logger.info("origin {} allowed {}".format(origin, os.environ["ALLOWED_ORIGINS"]))
    allowed_origins = os.environ["ALLOWED_ORIGINS"].split(",")
    return origin in allowed_origins


def get_response_headers(request_headers):
    response_headers = {}
    if request_headers:
        request_headers_normalized = dict((k.lower(), v) for k, v in request_headers.items())
        if "origin" in request_headers_normalized:
            origin = request_headers_normalized["origin"]
            if check_allowed_origin(origin):
                response_headers["Access-Control-Allow-Origin"] = origin
                response_headers["Access-Control-Allow-Credentials"] = True
    return response_headers


def get_http_response(**kwargs):
    return {
        **kwargs
    }


def issue_jwt(event, context):
    headers = dict((k.lower(), v) for k, v in event["headers"].items())
    try:
        if SecurityService.is_basic_auth(headers):
            username = SecurityService.get_basic_auth_authorized(headers)
            logging.info("Found {} in credentials".format(username))
            if username:
                ## token expires in 15 minutes = 15*60*1000 ms
                token = SecurityService.get_jwt_token(username)
                # return {"statusCode": 200, "body": json.dumps({"token": token})}
                return get_http_response(headers=get_response_headers(headers), statusCode=200,
                                         body=json.dumps({"token": token}))
            else:
                #return {"statusCode": 401}
                return get_http_response(headers=get_response_headers(headers), statusCode=401)
        else:
            return get_http_response(headers=get_response_headers(headers), statusCode=401)
    except Exception as e:
        print(f'Exception encountered: {e}')
        logging.error("exception encountered", e)
        return get_http_response(headers=get_response_headers(headers), statusCode=500)


def authorize(event, context):
    headers = dict((k.lower(), v) for k, v in event["headers"].items())
    logging.info("event is {}".format(event))
    http_method = event["httpMethod"]
    path = event["path"]
    method_arn = event["methodArn"]
    policy = generate_policy("anonymous_client", "Deny", "*")
    if AUTHORIZATION_FEATURE_ENABLED:
        logging.debug("headers = {}".format(headers))
        try:
            if SecurityService.is_bearer(headers):
                username = SecurityService.get_bearer_authorized(headers)
                logging.info("Found {} in credentials".format(username))
                if username:
                    policy = generate_policy(username, "Allow", "*")
            elif SecurityService.is_basic_auth(headers):
                username = SecurityService.get_basic_auth_authorized(headers)
                logging.info("Found {} in credentials".format(username))
                if username:
                    policy = generate_policy(username, "Allow", "*")
            elif SecurityService.is_api_key(headers):
                client_authorization = SecurityService.get_client_authorization_by_api_key(headers)
                logger.info("client authorization = {}".format(client_authorization))
                if client_authorization and SecurityService.check_scope(path, http_method,
                                                                        client_authorization.client_scopes):
                    policy = generate_policy(client_authorization.client_id, "Allow", "*")
            elif SecurityService.is_http_signature(headers):
                client_authorization = SecurityService.get_client_authorization_using_http_signature_authorized(
                    headers, http_method.lower(), path)
                if client_authorization and SecurityService.check_scope(path, http_method,
                                                                        client_authorization.client_scopes):
                    policy = generate_policy(client_authorization.client_id, "Allow", "*")
        except Exception as e:
            print(f'Exception encountered: {e}')
            logging.error("exception encountered", e)
            policy = generate_policy("anonymous_client", "Deny", "*")
    else:
        policy = generate_policy("anonymous_client", "Allow", "*")

    logging.info("policy is {}".format(policy))
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
