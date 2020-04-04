import collections
import logging
from typing import Collection, Mapping

from async_service import Service
from eth_utils.toolz import partition_all

from alexandria._utils import every, sha256, split_data_to_chunks
from alexandria.abc import ClientAPI, NetworkAPI, Node, RoutingTableAPI
from alexandria.constants import NODES_PER_PAYLOAD
from alexandria.messages import Message
from alexandria.payloads import (
    Advertise, Ack,
    Locate, Locations,
    Retrieve, Chunk,
)


CONTENT_ANNOUNCE_PERIOD = 600  # 10 minutes


class ContentManager(Service):
    logger = logging.getLogger('alexandria.content_manager.ContentManager')

    def __init__(self,
                 content_db: Mapping[bytes, bytes],
                 routing_table: RoutingTableAPI,
                 client: ClientAPI,
                 network: NetworkAPI,
                 ) -> None:
        self.content_db = content_db
        self.content_index = collections.defaultdict(set)
        self.client = client
        self.network = network

    async def run(self) -> None:
        ...

    async def send_advertise(self, node: Node, *, key: bytes) -> int:
        request_id = self.client.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Advertise(request_id, key),
            node,
        )
        self.logger.info("Sending %s", message)
        await self.client.message_dispatcher.send_message(message)
        return request_id

    async def send_ack(self, node: Node, *, request_id: int) -> None:
        request_id = self.client.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Ack(request_id),
            node,
        )
        self.logger.info("Sending %s", message)
        await self.client.message_dispatcher.send_message(message)

    async def send_locate(self, node: Node, *, key: bytes) -> int:
        request_id = self.client.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Locate(request_id, key),
            node,
        )
        self.logger.info("Sending %s", message)
        await self.client.message_dispatcher.send_message(message)
        return request_id

    async def send_locations(self,
                             node: Node,
                             *,
                             request_id: int,
                             locations: Collection[Node]) -> int:
        batches = tuple(partition_all(NODES_PER_PAYLOAD, locations))
        self.logger.info("Sending Locations with %d nodes to %s", len(locations), node)
        if batches:
            total_batches = len(batches)
            for batch in batches:
                payload = tuple(
                    (node.node_id, node.endpoint.ip_address.packed, node.endpoint.port)
                    for node in batch
                )
                response = Message(
                    Locations(request_id, total_batches, payload),
                    node,
                )
                await self.client.message_dispatcher.send_message(response)
            return total_batches
        else:
            response = Message(
                Locations(request_id, 1, ()),
                node,
            )
            await self.client.message_dispatcher.send_message(response)
            return 1

    async def send_retrieve(self,
                            node: Node,
                            *,
                            key: bytes) -> int:
        request_id = self.client.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Retrieve(request_id, key),
            node,
        )
        self.logger.info("Sending %s", message)
        await self.client.message_dispatcher.send_message(message)
        return request_id

    async def send_chunks(self,
                          node: Node,
                          *,
                          request_id: int,
                          data: bytes) -> int:
        if not data:
            response = Message(
                Chunk(request_id, 1, 0, b''),
                node,
            )
            await self.client.message_dispatcher.send_message(response)
            return 1

        all_chunks = split_data_to_chunks(data)
        total_chunks = len(all_chunks)

        for index, chunk in enumerate(all_chunks):
            response = Message(
                Chunk(request_id, total_chunks, index, chunk),
                node,
            )
            await self.client.message_dispatcher.send_message(response)

        return total_chunks

    #
    # Long Running Tasks
    #
    async def _periodically_report(self) -> None:
        async for _ in every(60):  # noqa: 841
            self.logger.info('Have %d index entries and %d content entries')

    async def _periodically_announce_content(self) -> None:
        async for _ in every(CONTENT_ANNOUNCE_PERIOD, initial_delay=60):  # noqa: F841
            for key, value in tuple(self.content_db):
                content_node_id = sha256(key)
                for node_id in self.routing_table.iter_nodes_around(content_node_id):
                    endpoint = self.endpoint_db.get_endpoints(node_id)
                    node = Node(node_id, endpoint)
                    await self.send_advertise(node, key=key, node=self.client.local_node)

    async def _handle_advertise_requests(self) -> None:
        with self.client.message_dispatcher.subscribe(Advertise) as subscription:
            while self.manager.is_running:
                request = await subscription.receive()
                payload = request.payload
                node_id, ip_address, port = payload.node
                content_node_id = sha256(payload.key)
                # TODO: ping the node to ensure it is available (unless it is the sending node).
                # TODO: verify content is actually available
                # TODO: check distance of key and store conditionally
                self.content_index[content_node_id].add(payload.node)
                await self.send_ack(request.node, request_id=payload.request_id)

    async def _handle_locate_requests(self) -> None:
        with self.client.message_dispatcher.subscribe(Locate) as subscription:
            while self.manager.is_running:
                request = await subscription.receive()
                payload = request.payload
                content_node_id = sha256(payload.key)
                # TODO: ping the node to ensure it is available (unless it is the sending node).
                # TODO: verify content is actually available
                # TODO: check distance of key and store conditionally
                locations = tuple(self.content_index[content_node_id])
                await self.send_locations(
                    request.node,
                    request_id=payload.request_id,
                    locations=locations,
                )

    async def _handle_get_requests(self) -> None:
        with self.client.message_dispatcher.subscribe(Retrieve) as subscription:
            while self.manager.is_running:
                request = await subscription.receive()
                payload = request.payload
                # TODO: ping the node to ensure it is available (unless it is the sending node).
                # TODO: verify content is actually available
                # TODO: check distance of key and store conditionally
                try:
                    data = self.content_db[payload.key]
                except KeyError:
                    data = b''

                await self.send_chunks(request.node, request_id=payload.request_id, data=data)
