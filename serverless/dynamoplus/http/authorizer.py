import os
from base64 import b64decode, b64encode

ROOT_ACCOUNT = os.getenv("ROOT_ACCOUNT")
ROOT_PASSWORD = os.getenv("ROOT_PASSWORD")


def authorize(event, context):
    whole_auth_token = event["headers"]["Authorization"]
    try:
        split = whole_auth_token.split(' ')
        if len(split) != 2 or split[0].strip().lower() != 'basic':
            raise Exception("unauthorized")
        try:
            username, password = b64decode(split[1]).decode().split(':', 1)
            if username == ROOT_ACCOUNT and password == ROOT_PASSWORD:
                return generate_policy(username, "Allow", "*")
        except Exception as e:
            print(f'Exception encountered: {e}')
            raise Exception('Unauthorized')

    except Exception as e:
        print(f'Exception encountered: {e}')
        raise Exception('Unauthorized')


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
