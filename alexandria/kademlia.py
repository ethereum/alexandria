import logging
import secrets
from typing import Iterable, Optional, Tuple

from async_service import Service
from eth_utils import encode_hex, to_tuple
import trio

from alexandria._utils import every, humanize_node_id, content_key_to_node_id
from alexandria.abc import (
    ClientAPI,
    ContentBundle,
    ContentManagerAPI,
    DurableDatabaseAPI,
    Endpoint,
    EndpointDatabaseAPI,
    KademliaAPI,
    NetworkAPI,
    Node,
    RoutingTableAPI,
)
from alexandria.config import KademliaConfig
from alexandria.constants import PING_TIMEOUT
from alexandria.content_manager import ContentManager
from alexandria.payloads import FindNodes, Ping, Advertise, Locate, Retrieve
from alexandria.routing_table import compute_distance
from alexandria.typing import NodeID


NodePayload = Tuple[NodeID, bytes, int]


class _EmptyFindNodesResponse(Exception):
    pass


class Kademlia(Service, KademliaAPI):
    logger = logging.getLogger('alexandria.kademlia.Kademlia')

    client: ClientAPI
    network: NetworkAPI
    routing_table: RoutingTableAPI
    content_manager: ContentManagerAPI

    def __init__(self,
                 routing_table: RoutingTableAPI,
                 endpoint_db: EndpointDatabaseAPI,
                 durable_db: DurableDatabaseAPI,
                 client: ClientAPI,
                 network: NetworkAPI,
                 config: KademliaConfig = None,
                 ) -> None:
        if config is None:
            config = KademliaConfig()
        self.config = config

        self.endpoint_db = endpoint_db
        self.routing_table = routing_table
        self.client = client
        self.network = network
        self.content_manager = ContentManager(
            center_id=self.client.local_node_id,
            durable_db=durable_db,
            config=config.storage_config,
        )

        (
            self._inbound_content_send_channel,
            self._inbound_content_receive_channel,
        ) = trio.open_memory_channel[Tuple[Node, bytes]](256)

    async def run(self) -> None:
        self.manager.run_daemon_task(self._periodic_report_routing_table_status)

        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._handle_lookup_requests)

        self.manager.run_daemon_task(self._handle_advertise_requests)
        self.manager.run_daemon_task(self._handle_locate_requests)
        self.manager.run_daemon_task(self._handle_retrieve_requests)

        self.manager.run_daemon_task(
            self._handle_content_ingestion,
            self._inbound_content_receive_channel,
        )
        self.manager.run_daemon_task(self._periodically_announce_content)

        self.manager.run_daemon_task(self._ping_occasionally)
        self.manager.run_daemon_task(self._lookup_occasionally)

        await self.manager.wait_finished()

    async def _periodic_report_routing_table_status(self) -> None:
        async for _ in every(300, 10):  # noqa: F841
            routing_stats = self.routing_table.get_stats()
            if routing_stats.full_buckets:
                full_buckets = '/'.join((str(index) for index in routing_stats.full_buckets))
            else:
                full_buckets = 'None'
            content_stats = self.content_manager.get_stats()
            self.logger.debug(
                (
                    "\n"
                    "###################[%s]#####################\n"
                    "       RoutingTable(bucket_size=%d, num_buckets=%d):\n"
                    "         - %d nodes\n"
                    "         - full buckets: %s\n"
                    "         - %d nodes in replacement cache\n"
                    "       ContentDB():\n"
                    "         - durable: %d\n"
                    "         - ephemeral-db: %d (%d / %d)\n"
                    "         - ephemeral-index: %d / %d\n"
                    "         - cache-db: %d (%d / %d)\n"
                    "         - cache-index: %d / %d\n"
                    "####################################################"
                ),
                humanize_node_id(self.client.local_node_id),
                routing_stats.bucket_size,
                routing_stats.num_buckets,
                routing_stats.total_nodes,
                full_buckets,
                routing_stats.num_in_replacement_cache,
                content_stats.durable_item_count,
                content_stats.ephemeral_db_count,
                content_stats.ephemeral_db_capacity,
                content_stats.ephemeral_db_total_capacity,
                content_stats.ephemeral_index_capacity,
                content_stats.ephemeral_index_total_capacity,
                content_stats.cache_db_count,
                content_stats.cache_db_capacity,
                content_stats.cache_db_total_capacity,
                content_stats.cache_index_capacity,
                content_stats.cache_index_total_capacity,
            )

    async def _handle_lookup_requests(self) -> None:
        with self.client.message_dispatcher.subscribe(FindNodes) as subscription:
            while self.manager.is_running:
                request = await subscription.receive()
                self.logger.debug("handling request: %s", request)

                found_nodes: Tuple[Node, ...]
                if request.payload.distance == 0:
                    found_nodes = (self.client.local_node,)
                else:
                    found_nodes = self._get_nodes_at_distance(request.payload.distance)

                self.logger.debug(
                    'found %d nodes for request: %s',
                    len(found_nodes),
                    request,
                )
                await self.client.send_found_nodes(
                    request.node,
                    request_id=request.payload.request_id,
                    found_nodes=found_nodes,
                )

    @to_tuple
    def _get_nodes_at_distance(self, distance: int) -> Iterable[Node]:
        """Send a Nodes message containing ENRs of peers at a given node distance."""
        nodes_at_distance = self.routing_table.get_nodes_at_log_distance(distance)

        for node_id in nodes_at_distance:
            endpoint = self.endpoint_db.get_endpoint(node_id)
            yield Node(node_id, endpoint)

    async def _verify_node(self, node: Node) -> None:
        with trio.move_on_after(PING_TIMEOUT) as scope:
            await self.network.verify_and_add(node)

        if scope.cancelled_caught:
            self.logger.debug('node verification timed out: %s', node)
        else:
            self.logger.debug('node verification succeeded: %s', node)

    async def _lookup_occasionally(self) -> None:
        async with trio.open_nursery() as nursery:
            async for _ in every(self.config.LOOKUP_INTERVAL):  # noqa: F841
                if self.routing_table.is_empty:
                    self.logger.debug('Aborting scheduled lookup due to empty routing table')
                    continue

                target_node_id = NodeID(secrets.randbits(256))
                found_nodes = await self.network.iterative_lookup(target_node_id)
                self.logger.debug(
                    'Lookup for %s yielded %d nodes',
                    humanize_node_id(target_node_id),
                    len(found_nodes),
                )
                for node in found_nodes:
                    if node.node_id == self.client.local_node_id:
                        continue
                    nursery.start_soon(self._verify_node, node)

    async def _ping_occasionally(self) -> None:
        async for _ in every(self.config.PING_INTERVAL):  # noqa: F841
            if self.routing_table.is_empty:
                self.logger.warning("Routing table is empty, no one to ping")
                continue

            log_distance = self.routing_table.get_least_recently_updated_log_distance()
            candidates = self.routing_table.get_nodes_at_log_distance(log_distance)
            for node_id in reversed(candidates):
                endpoint = self.endpoint_db.get_endpoint(node_id)
                node = Node(node_id, endpoint)

                with trio.move_on_after(PING_TIMEOUT) as scope:
                    await self.client.ping(node)

                if scope.cancelled_caught:
                    self.logger.debug(
                        'Node %s did not respond to ping.  Removing from routing table',
                        node_id,
                    )
                    self.routing_table.remove(node_id)
                else:
                    break

    async def _pong_when_pinged(self) -> None:
        with self.client.message_dispatcher.subscribe(Ping) as subscription:
            while self.manager.is_running:
                request = await subscription.receive()
                self.logger.debug(
                    "Got ping from %s, responding with pong",
                    request.node,
                )
                await self.client.send_pong(request.node, request_id=request.payload.request_id)

    async def _periodically_announce_content(self) -> None:
        async for _ in every(self.config.ANNOUNCE_INTERVAL, initial_delay=10):  # noqa: F841
            for key in self.content_manager.iter_content_keys():
                await self.network.announce(key, self.client.local_node)

    async def _handle_content_ingestion(self,
                                        receive_channel: trio.abc.ReceiveChannel[Tuple[Node, bytes]],  # noqa: E501
                                        ) -> None:
        async with receive_channel:
            async for node, key in receive_channel:
                data: Optional[bytes]
                if self._check_interest_in_ephemeral_content(key):
                    try:
                        with trio.fail_after(5):
                            data = await self.network.retrieve(node, key=key)
                            self.logger.debug(
                                'Successfully retrieved content: %s@%s',
                                encode_hex(key),
                                node,
                            )
                    except trio.TooSlowError:
                        self.logger.debug(
                            'Content retrieval timed out: %s@%s',
                            encode_hex(key),
                            node,
                        )
                        data = None
                else:
                    self.logger.debug('Not interested in content: %s@%s', encode_hex(key), node)
                    data = None
                bundle = ContentBundle(
                    key=key,
                    data=data,
                    node_id=node.node_id,
                )
                await trio.to_thread.run_sync(
                    self.content_manager.ingest_content,
                    bundle,
                )

    def _check_interest_in_ephemeral_content(self, key: bytes) -> bool:
        if self.content_manager.ephemeral_db.has_capacity:
            return True
        content_id = content_key_to_node_id(key)
        content_distance = compute_distance(content_id, self.client.local_node_id)
        furthest_key = sorted(
            self.content_manager.ephemeral_db.keys(),
            key=lambda key: compute_distance(self.client.local_node_id, content_key_to_node_id(key)),  # noqa: E501
            reverse=True,
        )[0]
        furthest_content_id = content_key_to_node_id(furthest_key)
        furthest_distance = compute_distance(furthest_content_id, self.client.local_node_id)
        return content_distance < furthest_distance

    async def _handle_advertise_requests(self) -> None:
        with self.client.message_dispatcher.subscribe(Advertise) as subscription:
            while self.manager.is_running:
                request = await subscription.receive()
                payload = request.payload
                await self.client.send_ack(request.node, request_id=payload.request_id)

                node = Node.from_payload(payload.node)

                # Queue the content to be ingested by the content manager
                try:
                    self._inbound_content_send_channel.send_nowait((node, payload.key))
                except trio.WouldBlock:
                    self.logger.error(
                        "Content processing channel is full.  Discarding "
                        "advertised content: %s@%s",
                        encode_hex(payload.key),
                        request.node,
                    )

    async def _handle_locate_requests(self) -> None:
        def get_endpoint(node_id: NodeID) -> Endpoint:
            try:
                return self.endpoint_db.get_endpoint(node_id)
            except KeyError:
                if node_id == self.client.local_node_id:
                    return self.client.listen_on
                else:
                    raise

        with self.client.message_dispatcher.subscribe(Locate) as subscription:
            while self.manager.is_running:
                request = await subscription.receive()
                payload = request.payload
                content_id = content_key_to_node_id(payload.key)
                # TODO: ping the node to ensure it is available (unless it is the sending node).
                # TODO: verify content is actually available
                # TODO: check distance of key and store conditionally
                location_ids = self.content_manager.get_index(content_id)

                locations = tuple(
                    Node(node_id, get_endpoint(node_id))
                    for node_id in location_ids
                )
                await self.client.send_locations(
                    request.node,
                    request_id=payload.request_id,
                    locations=locations,
                )

    async def _handle_retrieve_requests(self) -> None:
        with self.client.message_dispatcher.subscribe(Retrieve) as subscription:
            while self.manager.is_running:
                request = await subscription.receive()
                payload = request.payload
                # TODO: ping the node to ensure it is available (unless it is the sending node).
                # TODO: verify content is actually available
                # TODO: check distance of key and store conditionally
                try:
                    data = self.content_manager.get_content(payload.key)
                except KeyError:
                    data = b''

                await self.client.send_chunks(
                    request.node,
                    request_id=payload.request_id,
                    data=data,
                )
