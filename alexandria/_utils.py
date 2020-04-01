import hashlib

from eth_keys import keys

from eth_utils import humanize_hash
from alexandria.typing import NodeID


def humanize_node_id(node_id: NodeID):
    node_id_bytes = node_id.to_bytes(32, 'big')
    return humanize_hash(node_id_bytes)


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def public_key_to_node_id(public_key: keys.PublicKey) -> NodeID:
    return NodeID(int.from_bytes(sha256(public_key.to_bytes()), 'big'))
