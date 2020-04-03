from typing import Tuple

from ssz import sedes

from alexandria.typing import NodeID


class Ping(sedes.Serializable):
    fields = (
        ('request_id', sedes.uint16),
    )

    request_id: int

    def __str__(self) -> str:
        return f"Ping({self.request_id})"


class Pong(sedes.Serializable):
    fields = (
        ('request_id', sedes.uint16),
    )

    request_id: int

    def __str__(self) -> str:
        return f"Pong({self.request_id})"


class FindNodes(sedes.Serializable):
    fields = (
        ("request_id", sedes.uint16),
        ("distance", sedes.uint16),
    )

    request_id: int
    distance: int

    def __str__(self) -> str:
        return f"FindNodes({self.request_id}, {self.distance})"


class FoundNodes(sedes.Serializable):
    fields = (
        ("request_id", sedes.uint16),
        ("total", sedes.uint8),
        ("nodes", sedes.List(sedes.Container((
            sedes.uint256,
            sedes.bytes4,
            sedes.uint16,
        )), max_length=2**32))
    )

    request_id: int
    total: int
    nodes: Tuple[Tuple[NodeID, bytes, int]]

    def __str__(self) -> str:
        return f"FindNodes({self.request_id}, {self.total}, {len(self.nodes)}"
