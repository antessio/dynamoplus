import logging
from dynamoplus.repository.repositories import create_tables
logging.basicConfig(level=logging.INFO)

def admin(event, context):
        create_tables()
