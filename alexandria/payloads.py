from typing import Tuple, TYPE_CHECKING

from ssz import sedes

from alexandria._utils import content_key_to_graph_key, graph_key_to_content_key
from alexandria.sedes import byte_list, Maybe
from alexandria.typing import NodeID

if TYPE_CHECKING:
    from alexandria.abc import SGNodeAPI

NODE_SEDES = sedes.Container((
    sedes.uint256,
    sedes.bytes4,
    sedes.uint16,
))


class Ping(sedes.Serializable):  # type: ignore
    fields = (
        ('request_id', sedes.uint16),
    )

    request_id: int

    def __str__(self) -> str:
        return f"Ping({self.request_id})"


class Pong(sedes.Serializable):  # type: ignore
    fields = (
        ('request_id', sedes.uint16),
    )

    request_id: int

    def __str__(self) -> str:
        return f"Pong({self.request_id})"


class FindNodes(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("distance", sedes.uint16),
    )

    request_id: int
    distance: int

    def __str__(self) -> str:
        return f"FindNodes({self.request_id}, {self.distance})"


class FoundNodes(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("total", sedes.uint8),
        ("nodes", sedes.List(NODE_SEDES, max_length=2**32))
    )

    request_id: int
    total: int
    nodes: Tuple[Tuple[NodeID, bytes, int]]

    def __str__(self) -> str:
        return f"FindNodes({self.request_id}, {self.total}, {len(self.nodes)}"


class Advertise(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("key", byte_list),
        ("node", NODE_SEDES),
        # TODO: this should have a signature
    )

    request_id: int
    key: bytes


class Ack(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
    )

    request_id: int


class Locate(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("key", byte_list),
    )

    request_id: int
    key: bytes


class Locations(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("total", sedes.uint16),
        ("nodes", sedes.List(NODE_SEDES, max_length=2**32))
    )

    request_id: int
    total: int
    nodes: Tuple[Tuple[NodeID, bytes, int]]


class Retrieve(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("key", byte_list),
    )

    request_id: int
    key: bytes


class Chunk(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("total", sedes.uint16),
        ("index", sedes.uint16),
        ("data", byte_list),
    )

    request_id: int
    total: sedes.uint16
    index: sedes.uint16
    data: bytes


#
# Skip Graph
#
class SkipGraphNode(sedes.Serializable):  # type: ignore
    fields = (
        ("key", byte_list),
        ("neighbors_left", sedes.List(byte_list, max_length=2**32)),
        ("neighbors_right", sedes.List(byte_list, max_length=2**32)),
    )

    key: bytes
    neighbors_left: Tuple[bytes, ...]
    neighbors_right: Tuple[bytes, ...]

    @classmethod
    def from_sg_node(cls, sg_node: 'SGNodeAPI') -> 'SkipGraphNode':
        return cls(
            key=graph_key_to_content_key(sg_node.key),
            neighbors_left=tuple(
                graph_key_to_content_key(neighbor) for neighbor in sg_node.neighbors[0]
            ),
            neighbors_right=tuple(
                graph_key_to_content_key(neighbor) for neighbor in sg_node.neighbors[1]
            ),
        )

    def to_sg_node(self) -> 'SGNodeAPI':
        from alexandria.skip_graph import SGNode
        return SGNode(
            key=content_key_to_graph_key(self.key),
            neighbors_left=tuple(
                content_key_to_graph_key(neighbor) for neighbor in self.neighbors_left
            ),
            neighbors_right=tuple(
                content_key_to_graph_key(neighbor) for neighbor in self.neighbors_right
            ),
        )


class GraphGetIntroduction(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
    )

    request_id: int


class GraphIntroduction(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("nodes", sedes.List(SkipGraphNode, max_length=2**32)),
    )

    request_id: int
    nodes: Tuple[SkipGraphNode, ...]


class GraphGetNode(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("key", byte_list),
    )

    request_id: int
    key: bytes


class GraphNode(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("node", Maybe(SkipGraphNode)),
    )

    request_id: int
    node: SkipGraphNode


class GraphInsert(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("key", byte_list),
    )

    request_id: int
    key: bytes


class GraphInserted(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
    )

    request_id: int


class GraphDelete(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
        ("key", byte_list),
    )

    request_id: int
    key: bytes


class GraphDeleted(sedes.Serializable):  # type: ignore
    fields = (
        ("request_id", sedes.uint16),
    )

    request_id: int
