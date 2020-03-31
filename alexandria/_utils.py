from eth_utils import humanize_hash
from alexandria.typing import NodeID


def humanize_node_id(node_id: NodeID):
    node_id_bytes = node_id.to_bytes(32, 'big')
    return humanize_hash(node_id_bytes)
