import json
import logging
from dynamoplus.repository.repositories import create_tables
logging.basicConfig(level=logging.INFO)

def setup(event, context):
        create_tables()
        return {"statusCode":200}

def info(event, context):
        info = {"version": "0.4"}
        return {"statusCode":200, "body": json.dumps(info)}
