import logging
from enum import Enum

from aws.dynamodb.dynamodb_repository import DynamoDBRepository
from dotenv import load_dotenv
# from your_library import your_function  # Replace with actual import
from dynamoplus import dynamo_plus_v2
from flask import Flask, jsonify, request
from flask_login import LoginManager, UserMixin, AnonymousUserMixin, login_required
from werkzeug.exceptions import Unauthorized

load_dotenv()


def create_app():
    app = Flask(__name__)

    # Set up the logging configuration
    app.logger.setLevel(logging.INFO)  # Adjust the log level as needed

    # Configure the log format
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Configure the log handler (you can use different handlers such as StreamHandler, FileHandler, etc.)
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(log_formatter)

    app.logger.addHandler(log_handler)
    app.json.sort_keys = False
    login_manager = LoginManager()
    login_manager.init_app(app)

    @login_manager.request_loader
    def load_user_from_request(request):
        user_id = dynamoplus.authorize(dict(request.headers), request.method, request.path)
        if user_id:
            return DynamoPlusUser(user_id)
        else:
            return AnonymousUserMixin()

    return app


app = create_app()

system_repository = DynamoDBRepository('system')
domain_repository = DynamoDBRepository('domain')
dynamoplus = dynamo_plus_v2.Dynamoplus(
    system_repository,
    system_repository,
    system_repository,
    system_repository,
    system_repository,
    domain_repository
)


class AuthenticationType(Enum):
    BASIC_AUTH = "BASIC_AUTH"
    HTTP_SIGNATURE = "HTTP_SIGNATURE"
    API_KEY = "API_KEY"


class DynamoPlusUser(UserMixin):
    def __init__(self, id: str):
        self.id = id


@app.route('/dynamoplus/system/info')
def system_info():
    return dynamoplus.info()


@app.route('/dynamoplus/admin/setup', methods=['POST'])
def setup():
    system_repository.create_table()
    domain_repository.create_table()
    return '', 201


@app.route('/dynamoplus/admin/cleanup', methods=['POST'])
def cleanup():
    system_repository.cleanup_table()
    domain_repository.cleanup_table()
    return '', 201


@app.route('/dynamoplus/<collection_name>/<id>', methods=['GET'])
@login_required
def get(collection_name: str, id: str):
    limit = request.args.get('limit', default=20, type=int)
    last_key = request.args.get('last_key', default=None, type=str)
    document = dynamoplus.get(collection_name, id)
    if document:
        return jsonify(document)
    else:
        # Return a 404 response if the collection is not found
        return jsonify({'error': '%s - %s not found'.format(collection_name, id)}), 404


@app.route('/dynamoplus/<collection_name>', methods=['GET'])
@login_required
def get_all(collection_name: str):
    limit = request.args.get('limit', default=20, type=int)
    last_key = request.args.get('last_key', default=None, type=str)
    documents, last_element_key = dynamoplus.get_all(collection_name, last_key, limit)

    return jsonify({
        "has_more": last_element_key is not None,
        "data": documents
    })


@app.route('/dynamoplus/<collection_name>', methods=['POST'])
@login_required
def add_to_collection(collection_name: str):
    data = request.get_json()  # Parse JSON data from the request body
    created_document = dynamoplus.create(collection_name, data)
    return jsonify(created_document), 201


@app.route('/dynamoplus/<collection_name>/<document_id>', methods=['PUT'])
@login_required
def update_collection(collection_name: str, document_id: str):
    data = request.get_json()  # Parse JSON data from the request body
    updated_document = dynamoplus.update(collection_name, data, document_id)
    return jsonify(updated_document), 201


@app.route('/dynamoplus/<collection_name>/query', methods=['POST'])
@login_required
def query_collection(collection_name: str):
    limit = request.args.get('limit', default=20, type=int)
    start_from = request.args.get('last_key', default=None, type=str)
    query = request.get_json()  # Parse JSON data from the request body
    documents, last_element_key = dynamoplus.query(collection_name, query, start_from, limit)

    return jsonify({
        "has_more": last_element_key is not None,
        "data": documents
    })


@app.route('/dynamoplus/<collection_name>/<document_id>', methods=['DELETE'])
@login_required
def delete_collection(collection_name: str, document_id: str):
    ##  delete doesn't use entity name and id, the same gets in the system service
    dynamoplus.delete(collection_name, document_id)
    return '', 201


# Define a custom error handler for a specific exception (e.g., CustomException)
@app.errorhandler(SyntaxError)
def handle_custom_exception(e):
    response = jsonify({'error': str(e)})
    response.status_code = 400  # Map CustomException to a 400 Bad Request error
    return response


# Define a general error handler for all other exceptions
@app.errorhandler(Exception)
def handle_generic_exception(e):
    response = jsonify({'error': 'Internal Server Error: {0}'.format(str(e))})
    response.status_code = 500  # Map all other exceptions to a 500 Internal Server Error
    return response


@app.errorhandler(Unauthorized)
def handle_unauthorized(e):
    response = jsonify({'error': ' {0}'.format(str(e))})
    response.status_code = 401
    return response


if __name__ == '__main__':
    app.run(debug=True)