from typing import Tuple

from ssz import sedes

from alexandria.sedes import byte_list
from alexandria.typing import NodeID

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
