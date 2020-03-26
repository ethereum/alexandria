from typing import Tuple

from alexandria.abc import NodeDatabaseAPI
from alexandria.typing import NodeID


class NodeDatabase(NodeDatabaseAPI):
    async def get_bootnodes(self) -> Tuple[NodeID, ...]:
        ...
