from enum import Enum
from typing import *

from dynamoplus.utils.utils import auto_str


class ScopesType(Enum):
    QUERY = "query",
    CREATE = "create",
    GET = "get",
    UPDATE = "update",
    DELETE = "delete"


@auto_str
class Scope(object):
    _scope_type: ScopesType
    _collection_name: str

    def __init__(self, collection_name: str, scope_type: ScopesType):
        self._collection_name = collection_name
        self._scope_type = scope_type

    @property
    def collection_name(self):
        return self._collection_name

    @collection_name.setter
    def collection_name(self, value: str):
        self._collection_name = value

    @property
    def scope_type(self):
        return self._scope_type

    @scope_type.setter
    def scope_type(self, value: ScopesType):
        self._scope_type = value



@auto_str
class ClientAuthorization(object):
    _client_id: str
    _client_scopes: List[Scope]

    def __init__(self, client_id: str, client_scopes: List[Scope]):
        self._client_id = client_id
        self._client_scopes = client_scopes

    @property
    def client_id(self):
        return self._client_id

    @client_id.setter
    def client_id(self, value: str):
        self._client_id = value

    @property
    def client_scopes(self):
        return self._client_scopes

    @client_scopes.setter
    def client_scopes(self, value: List[Scope]):
        self._client_scopes = value


@auto_str
class ClientAuthorizationApiKey(ClientAuthorization):
    _api_key: str
    _whitelist_hosts: List[str]

    def __init__(self, client_id: str, client_scopes: List[Scope], api_key: str, whitelist_hosts: List[str]):
        super().__init__(client_id, client_scopes)
        self._api_key = api_key
        self._whitelist_hosts = whitelist_hosts

    @property
    def api_key(self):
        return self._api_key

    @api_key.setter
    def api_key(self, value: str):
        self._whitelist_hosts = value

    @property
    def whitelist_hosts(self):
        return self._whitelist_hosts

    @whitelist_hosts.setter
    def whitelist_hosts(self, value: List[str]):
        self._whitelist_hosts = value


@auto_str
class ClientAuthorizationHttpSignature(ClientAuthorization):
    _client_public_key: str

    def __init__(self, client_id: str, client_scopes: List[Scope], client_public_key: str):
        super().__init__(client_id, client_scopes)
        self._client_public_key = client_public_key

    @property
    def client_public_key(self):
        return self._client_public_key

    @client_public_key.setter
    def client_public_key(self, value: str):
        self._client_public_key = value

    def __str__(self) -> str:
        return super().__str__()
