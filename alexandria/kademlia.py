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
    DurableDatabaseAPI,
    Endpoint,
    EndpointDatabaseAPI,
    KademliaAPI,
    NetworkAPI,
    Node,
    RoutingTableAPI,
)
from alexandria.config import KademliaConfig
from alexandria.constants import (
    PING_TIMEOUT,
    ANNOUNCE_TIMEOUT,
    KADEMLIA_ANNOUNCE_INTERVAL,
    KADEMLIA_ANNOUNCE_CONCURRENCY,
)
from alexandria.content_manager import AdvertiseTracker, ContentManager
from alexandria.payloads import FindNodes, Ping, Advertise, Locate, Retrieve
from alexandria.routing_table import compute_distance
from alexandria.skip_graph import GraphDB
from alexandria.typing import NodeID


NodePayload = Tuple[NodeID, bytes, int]


class _EmptyFindNodesResponse(Exception):
    pass


class Kademlia(Service, KademliaAPI):
    logger = logging.getLogger('alexandria.kademlia.Kademlia')

    _network_graph: Optional[GraphAPI] = None

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
        self.advertise_tracker = AdvertiseTracker(KADEMLIA_ANNOUNCE_INTERVAL)

        (
            self._inbound_content_send_channel,
            self._inbound_content_receive_channel,
        ) = trio.open_memory_channel[Tuple[Node, bytes]](256)

        self.graph_db = GraphDB()
        self._local_graph = LocalGraph(SGNode(self.client.local_node_id))

    @property
    def graph(self) -> GraphAPI:
        if self._network_graph is None:
            return self._local_graph
        else:
            return self._network_graph

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
        self.manager.run_daemon_task(self._periodically_advertise_content)

        self.manager.run_daemon_task(self._ping_occasionally)
        self.manager.run_daemon_task(self._lookup_occasionally)

        self.manager.run_task(self._initialize_skip_graph)
        self.manager.run_daemon_task(self._monitor_skip_graph)
        self.manager.run_daemon_task(self._handle_introduction_requests)
        self.manager.run_daemon_task(self._handle_link_nodes_requests)
        self.manager.run_daemon_task(self._handle_get_graph_node_requests)

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

    async def _initialize_network_graph(self) -> GraphAPI:
        while self.manager.is_running:
            random_content_id = secrets.randbits(256)
            candidates = await self.network.iterative_lookup(random_content_id):
            for node in candidates:
                try:
                    with trio.fail_after(INTRODUCTION_TIMEOUT):
                        introduction_nodes = await self.network.get_introduction(node)
                except trio.TooSlowError:
                    continue

                if len(introduction_nodes) == 0:
                    continue

                for sg_node in introduction_nodes:
                    self.graph_db.set(sg_node.key, sg_node)

                self.logger.info("Network SkipGraph initialized")
                return NetworkGraph(self.graph_db, introduction_nodes[0], self.network)
            else:
                self.logger.debug(
                    f"Failed to initialize network Skip Graph after querying "
                    f"{len(candidates)} nodes."
                )

    async def _initialize_skip_graph(self) -> None:
        # 1. Get an introduction to the network's SkipGraph
        self._network_graph = await self._initialize_network_graph()

        # 2. Transfer everything from the temporary "local" SkipGraph into the
        # network one.
        for key in self._local_graph.db.keys():
            if key == self.client.local_node_id:
                continue
            try:
                # TODO: this should be done concurrently
                await self._network_graph.insert(key)
            except AlreadyPresent:
                continue

        # 3. Ensure the network graph is populated with all of our local
        # content.
        ...

    async def _monitor_skip_graph(self) -> None:
        # A: broken neighbor links to unretrievable nodes.
        # B: nodes with unretrievable data.
        ...

    async def _handle_introduction_requests(self) -> None:
        async with self.client.message_dispatcher.subscribe(GraphGetIntroduction) as subscription:
            async for request in subscription:
                self.logger.debug("handling request: %s", request)
                await self.client.send_graph_introduction(
                    request.node,
                    request_id=request.payload.request_id,
                    graph_nodes=(self.graph.cursor,),
                )

    async def _handle_link_nodes_requests(self) -> None:
        async with self.client.message_dispatcher.subscribe(GraphLinkNodes) as subscription:
            async for request in subscription:
                self.logger.debug("handling request: %s", request)
                # Acknowledge the request first, then do the linking.
                await self.client.send_graph_linked(
                    request.node,
                    request_id=request.payload.request_id,
                )
                payload = request.payload
                left_key = payload.left if payload.left is None else big_endian_to_int(payload.left)
                right_key = payload.right if payload.right is None else big_endian_to_int(payload.right)  # noqa: E501
                level = payload.level

                if left_key is not None:
                    try:
                        left = self.graph_db.get(left_key)
                    except KeyError:
                        pass
                    else:
                        # TODO: level could be invalid
                        left.set_right_neighbor(right_key, level)

                if right_key is not None:
                    try:
                        right = self.graph_db.get(right_key)
                    except KeyError:
                        pass
                    else:
                        # TODO: level could be invalid
                        right.set_left_neighbor(left_key, level)

    async def _handle_get_graph_node_requests(self) -> None:
        async with self.client.message_dispatcher.subscribe(GraphLinkNodes) as subscription:
            async for request in subscription:
                self.logger.debug("handling request: %s", request)
                # Acknowledge the request first, then do the linking.
                key = big_endian_to_int(request.payload.key)

                sg_node: Optional[SGNode]
                try:
                    sg_node = self.graph_db.get(key)
                except KeyError:
                    node = None

                await self.client.send_graph_node(
                    request.node,
                    request_id=request.payload.request_id,
                    sg_node=sg_node,
                )

    async def _handle_lookup_requests(self) -> None:
        async with self.client.message_dispatcher.subscribe(FindNodes) as subscription:
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
                    nursery.start_soon(self.network.verify_and_add, node)

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
        async with self.client.message_dispatcher.subscribe(Ping) as subscription:
            while self.manager.is_running:
                request = await subscription.receive()
                self.logger.debug(
                    "Got ping from %s, responding with pong",
                    request.node,
                )
                await self.client.send_pong(request.node, request_id=request.payload.request_id)

    async def _periodically_advertise_content(self) -> None:
        def enqueue_all_content_keys() -> None:
            self.logger.debug("Starting load of all content database into advertisement queue")
            for key in self.content_manager.iter_content_keys():
                self.advertise_tracker.enqueue(key, 0.0)
            self.logger.debug("Finished load of all content database into advertisement queue")

        await trio.to_thread.run_sync(enqueue_all_content_keys)

        semaphor = trio.Semaphore(
            KADEMLIA_ANNOUNCE_CONCURRENCY,
            max_value=KADEMLIA_ANNOUNCE_CONCURRENCY,
        )

        async def do_announce(key):
            with trio.move_on_after(ANNOUNCE_TIMEOUT):
                await self.network.announce(
                    key,
                    Node(self.client.local_node_id, self.client.external_endpoint),
                )
            self.advertise_tracker.enqueue(key)
            semaphor.release()

        async with trio.open_nursery() as nursery:
            while self.manager.is_running:
                key = await self.advertise_tracker.pop_next()
                try:
                    self.content_manager.get_content(key)
                except KeyError:
                    self.logger.debug(
                        'Discarding announcement key missing content: %s',
                        encode_hex(key),
                    )
                    continue

                self.logger.debug('Announcing: %s', encode_hex(key))

                await semaphor.acquire()
                nursery.start_soon(do_announce, key)

    async def _ingest_content(self, node: Node, key: bytes) -> None:
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
        self.content_manager.ingest_content(bundle)
        if bundle.data:
            self.advertise_tracker.enqueue(key, last_advertised_at=0.0)

    async def _handle_content_ingestion(self,
                                        receive_channel: trio.abc.ReceiveChannel[Tuple[Node, bytes]],  # noqa: E501
                                        ) -> None:
        async with trio.open_nursery() as nursery:
            async with receive_channel:
                async for node, key in receive_channel:
                    nursery.start_soon(self._ingest_content, node, key)

    def _check_interest_in_ephemeral_content(self, key: bytes) -> bool:
        if self.content_manager.durable_db.has(key):
            return False
        elif self.content_manager.ephemeral_db.has(key):
            return False
        elif self.content_manager.ephemeral_db.has_capacity:
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
        async with self.client.message_dispatcher.subscribe(Advertise) as subscription:
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
                    return self.client.external_endpoint
                else:
                    raise

        async with self.client.message_dispatcher.subscribe(Locate) as subscription:
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
        async with self.client.message_dispatcher.subscribe(Retrieve) as subscription:
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
