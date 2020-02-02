import jwt
import json
import os

from cryptography.hazmat.backends import default_backend
from cryptography.x509 import load_pem_x509_certificate


# Set by serverless.yml
AUTH0_CLIENT_ID = os.getenv('AUTH0_CLIENT_ID')
AUTH0_CLIENT_PUBLIC_KEY = os.getenv('AUTH0_CLIENT_PUBLIC_KEY')
AUDIENCE = os.getenv('AUDIENCE')

def authorize(event, context):
    whole_auth_token = event["headers"]["Authorization"]
    if not whole_auth_token:
        raise Exception('Unauthorized')

    print('Client token: ' + whole_auth_token)
    print('Method ARN: ' + event['methodArn'])
    collection = event["pathParameters"]["collection"]
    print("collection {}".format(collection))
    token_parts = whole_auth_token.split(' ')
    auth_token = token_parts[1]
    token_method = token_parts[0]

    if not (token_method.lower() == 'bearer' and auth_token):
        print("Failing due to invalid token_method or missing auth_token")
        raise Exception('Unauthorized')

    try:
        payload = jwt_verify(auth_token, AUTH0_CLIENT_PUBLIC_KEY)
        print("Auth0 payload "+str(payload))
        principal_id = payload['sub']
        ## TODO: given the principal_id (and its scopes) and a request path, check authorization
        policy = generate_policy(principal_id, 'Allow', "*")
        return policy
    except Exception as e:
        print(f'Exception encountered: {e}')
        raise Exception('Unauthorized')
def jwt_verify(auth_token,public_key):
    public_key = format_public_key(public_key)
    pub_key = convert_certificate_to_pem(public_key)
    payload = jwt.decode(auth_token, pub_key, algorithms=['RS256'], audience=AUDIENCE)
    return payload

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


def convert_certificate_to_pem(public_key):
    cert_str = public_key.encode()
    cert_obj = load_pem_x509_certificate(cert_str, default_backend())
    pub_key = cert_obj.public_key()
    return pub_key


def format_public_key(public_key):
    public_key = public_key.replace('\n', ' ').replace('\r', '')
    public_key = public_key.replace('-----BEGIN CERTIFICATE-----', '-----BEGIN CERTIFICATE-----\n')
    public_key = public_key.replace('-----END CERTIFICATE-----', '\n-----END CERTIFICATE-----')
    return public_key