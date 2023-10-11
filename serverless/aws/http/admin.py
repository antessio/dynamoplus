try:
  import unzip_requirements
except ImportError:
  pass
import logging

## TODO: use an interface and not directly the repo
from dynamodb.aws.dynamodb.dynamodbdao import create_tables,cleanup_tables


logging.basicConfig(level=logging.INFO)


def setup(event, context):
    logging.info("setup {}".format(event))
    create_tables()
    return {"statusCode": 200}

def cleanup(event,context):
    cleanup_tables()
    return {"statusCode": 200}