import pathlib
from typing import Tuple

from eth_utils import encode_hex, decode_hex
from eth_utils.curried import (
    apply_formatter_to_array,
    apply_formatter_at_index,
)
from web3 import Web3
from web3.providers import IPCProvider
from web3.module import Module
from web3.types import RPCEndpoint

from alexandria.abc import Node, RoutingTableStats, ContentStats, BucketInfo
from alexandria.xdg import get_xdg_alexandria_root
from alexandria.rpc import (
    node_to_rpc,
    node_from_rpc,
)


class RPC:
    alexandria_nodeId = RPCEndpoint('alexandria_nodeId')
    alexandria_routingTableStats = RPCEndpoint('alexandria_routingTableStats')
    alexandria_routingTableBucketInfo = RPCEndpoint('alexandria_routingTableBucketInfo')

    # network methods
    alexandria_ping = RPCEndpoint('alexandria_ping')
    alexandria_findNodes = RPCEndpoint('alexandria_findNodes')
    alexandria_advertise = RPCEndpoint('alexandria_advertise')
    alexandria_locate = RPCEndpoint('alexandria_locate')
    alexandria_retrieve = RPCEndpoint('alexandria_retrieve')

    # content
    alexandria_contentStats = RPCEndpoint('alexandria_contentStats')
    alexandria_addContent = RPCEndpoint('alexandria_addContent')


class AlexandriaModule(Module):
    #
    # Basic Info
    #
    def get_local_node_id(self) -> Node:
        node_payload = self.web3.manager.request_blocking(RPC.alexandria_nodeId, [])
        node = node_from_rpc(node_payload)
        return node

    def get_routing_table_stats(self) -> RoutingTableStats:
        response = self.web3.manager.request_blocking(RPC.alexandria_routingTableStats, [])
        return RoutingTableStats(*apply_formatter_at_index(tuple, 3, response))

    def get_routing_table_bucket_info(self, bucket_index: int) -> BucketInfo:
        # TODO: normalize hex encoded node ids
        response = self.web3.manager.request_blocking(
            RPC.alexandria_routingTableBucketInfo,
            (bucket_index,),
        )
        return BucketInfo(*apply_formatter_at_index(
            tuple, 3,
            apply_formatter_at_index(
                tuple, 2,
                response,
            ),
        ))

    #
    # Client/Network API
    #
    def ping(self, node: Node) -> float:
        payload = node_to_rpc(node)
        return float(self.web3.manager.request_blocking(RPC.alexandria_ping, payload))

    def find_nodes(self, node: Node, distance: int) -> Tuple[Node, ...]:
        payload = node_to_rpc(node) + (distance,)
        found_nodes = self.web3.manager.request_blocking(RPC.alexandria_findNodes, payload)
        return tuple(
            node_from_rpc(node_payload)
            for node_payload in found_nodes
        )

    def advertise(self, node: Node, key: bytes, who: Node) -> float:
        payload = node_to_rpc(node) + (encode_hex(key),) + node_to_rpc(who)
        return float(self.web3.manager.request_blocking(RPC.alexandria_advertise, payload))

    def locate(self, node: Node, key: bytes) -> Tuple[Node, ...]:
        payload = node_to_rpc(node) + (encode_hex(key),)
        return apply_formatter_to_array(  # type: ignore
            node_from_rpc,
            self.web3.manager.request_blocking(RPC.alexandria_locate, payload),
        )

    def retrieve(self, node: Node, key: bytes) -> bytes:
        payload = node_to_rpc(node) + (encode_hex(key),)
        data_as_hex = self.web3.manager.request_blocking(RPC.alexandria_retrieve, payload)
        return decode_hex(data_as_hex)

    #
    # Content Management
    #
    def get_content_stats(self) -> ContentStats:
        response = self.web3.manager.request_blocking(RPC.alexandria_contentStats, [])
        return ContentStats(*response)

    def add_content(self, key: bytes, data: bytes, is_ephemeral: bool) -> None:
        payload = (encode_hex(key), encode_hex(data), is_ephemeral)
        self.web3.manager.request_blocking(RPC.alexandria_addContent, payload)


def get_w3(ipc_path: pathlib.Path = None) -> Web3:
    if ipc_path is None:
        ipc_path = get_xdg_alexandria_root() / 'jsonrpc.ipc'

    provider = IPCProvider(str(ipc_path))
    w3 = Web3(provider=provider, modules={'alexandria': (AlexandriaModule,)})
    return w3
