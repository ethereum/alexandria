import hashlib

from p2p._utils import (
    sxor,
)

from alexandria.typing import (
    NodeID,
    Tag,
)


def _sxor(s1: bytes, s2: bytes) -> bytes:
    if len(s1) != len(s2):
        raise ValueError("Cannot sxor strings of different length")
    return bytes(x ^ y for x, y in zip(s1, s2))


def compute_tag(source_node_id: NodeID, destination_node_id: NodeID) -> Tag:
    """Compute the tag used in message packets sent between two nodes."""
    destination_node_id_hash = hashlib.sha256(destination_node_id.to_bytes(32, 'big')).digest()
    tag = sxor(destination_node_id_hash, source_node_id.to_bytes(32, 'big'))
    return Tag(tag)


def recover_source_id_from_tag(tag: Tag, destination_node_id: NodeID) -> NodeID:
    """Recover the node id of the source from the tag in a message packet."""
    destination_node_id_hash = hashlib.sha256(destination_node_id.to_bytes(32, 'big')).digest()
    source_node_id_bytes = _sxor(tag, destination_node_id_hash)
    return NodeID(int.from_bytes(source_node_id_bytes, 'big'))
