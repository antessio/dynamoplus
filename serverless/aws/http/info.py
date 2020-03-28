import json
import logging
VERSION = "0.4.0"
logging.basicConfig(level=logging.INFO)


def info(event, context):
    system_info = {"version": VERSION}
    return {"statusCode": 200, "body": json.dumps(system_info)}