from typing import Dict

from alexandria.abc import Endpoint, EndpointDatabaseAPI
from alexandria.typing import NodeID


class MemoryEndpointDB(EndpointDatabaseAPI):
    _db: Dict[NodeID, Endpoint]

    def __init__(self, db: Dict[NodeID, Endpoint] = None) -> None:
        if db is None:
            db = {}
        self._db = db

    def has_endpoint(self, node_id: NodeID) -> bool:
        return node_id in self._db

    def get_endpoint(self, node_id: NodeID) -> Endpoint:
        return self._db[node_id]

    def set_endpoint(self, node_id: NodeID, endpoint: Endpoint) -> None:
        self._db[node_id] = endpoint
