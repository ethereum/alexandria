import ipaddress
import pathlib
from typing import Any, Mapping

from eth_utils import to_int

from web3 import Web3
from web3.providers import IPCProvider
from web3.module import Module
from web3.types import RPCEndpoint

from alexandria._utils import node_id_to_hex
from alexandria.abc import Endpoint, Node
from alexandria.typing import NodeID
from alexandria.xdg import get_xdg_alexandria_root


class RPC:
    alexandria_bond = RPCEndpoint('alexandria_bond')
    alexandria_nodeId = RPCEndpoint('alexandria_nodeId')
    alexandria_routingTableInfo = RPCEndpoint('alexandria_routingTableInfo')
    alexandria_routingTableBucket = RPCEndpoint('alexandria_routingTableBucket')


class AlexandriaModule(Module):
    def bond(self, node: Node) -> float:
        payload = (
            node_id_to_hex(node.node_id),
            str(node.endpoint.ip_address),
            node.endpoint.port,
        )
        return float(self.web3.manager.request_blocking(RPC.alexandria_bond, payload))

    def get_local_node_id(self) -> Node:
        node_payload = self.web3.manager.request_blocking(RPC.alexandria_nodeId, [])
        node_id_hex, ip_address, port = node_payload
        endpoint = Endpoint(ipaddress.IPv4Address(ip_address), port)
        return Node(NodeID(to_int(hexstr=node_id_hex)), endpoint)

    def get_routing_table_info(self) -> Mapping[Any, Any]:
        return self.web3.manager.request_blocking(RPC.alexandria_routingTableInfo, [])  # type: ignore  # noqa: E501

    def get_routing_table_bucket(self, bucket_index: int) -> Mapping[Any, Any]:
        return self.web3.manager.request_blocking(RPC.alexandria_routingTableBucket, [bucket_index])  # type: ignore  # noqa: E501


def get_w3(ipc_path: pathlib.Path = None) -> Web3:
    if ipc_path is None:
        ipc_path = get_xdg_alexandria_root() / 'jsonrpc.ipc'

    provider = IPCProvider(str(ipc_path))
    w3 = Web3(provider=provider, modules={'alexandria': (AlexandriaModule,)})
    return w3
