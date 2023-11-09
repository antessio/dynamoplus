import awsgi
from app import app

def lambda_handler(event, context):
    return awsgi.response(app, event, context)