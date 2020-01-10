import os
from dynamoplus.service.authorization.authorization import AuthorizationService

AUTHORIZATION_FEATURE_ENABLED = bool(os.getenvb("AUTHORIZATION_FEATURE_ENABLED","False"))
ROOT_ACCOUNT = os.getenv("ROOT_ACCOUNT")
ROOT_PASSWORD = os.getenv("ROOT_PASSWORD")


def authorize(event, context):
    headers = event["headers"]
    http_method = event["httpMethod"]
    path = event["path"]
    method_arn = event["methodArn"]
    if AUTHORIZATION_FEATURE_ENABLED:
        try:
            if AuthorizationService.is_basic_auth(headers):
                username = AuthorizationService.get_basic_auth_authorized(headers)
                if username:
                    return generate_policy(username, "Allow", "*")
            elif AuthorizationService.is_api_key(headers):
                client_authorization = AuthorizationService.get_client_authorization_by_api_key(headers)
                if AuthorizationService.check_scope(path,http_method,client_authorization.client_scopes):
                    return generate_policy(client_authorization.client_id, "Allow", "*")
            elif AuthorizationService.is_http_signature(headers):
                client_authorization = AuthorizationService.get_client_authorization_using_http_signature_authorized(headers, http_method,path)
                if AuthorizationService.check_scope(path, http_method, client_authorization.client_scopes):
                    return generate_policy(client_authorization.client_id, "Allow", "*")
        except Exception as e:
            print(f'Exception encountered: {e}')
            raise Exception('Unauthorized')
    else:
        generate_policy("anonymous_client", "Allow", "*")


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
