try:
  import unzip_requirements
except ImportError:
  pass

import json
import logging
VERSION = "0.5.0.1xxxs"
logging.basicConfig(level=logging.INFO)


def info(event, context):
    system_info = {"version": VERSION}
    return {"statusCode": 200, "body": json.dumps(system_info)}