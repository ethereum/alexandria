import logging
import secrets
from typing import Iterator, Optional, Tuple

from async_service import Service
from eth_utils import (
    encode_hex,
    to_tuple,
)
import trio

from alexandria._utils import (
    every,
    humanize_node_id,
    content_key_to_node_id,
    content_key_to_graph_key,
)
from alexandria.abc import (
    ClientAPI,
    ContentBundle,
    DurableDatabaseAPI,
    Endpoint,
    EndpointDatabaseAPI,
    GraphAPI,
    KademliaAPI,
    NetworkAPI,
    Node,
    RoutingTableAPI,
    SGNodeAPI,
)
from alexandria.config import KademliaConfig
from alexandria.constants import (
    PING_TIMEOUT,
    ANNOUNCE_TIMEOUT,
    KADEMLIA_ANNOUNCE_INTERVAL,
    INTRODUCTION_TIMEOUT,
    INSERT_TIMEOUT,
)
from alexandria.content_manager import ContentManager
from alexandria.exceptions import ContentNotFound
from alexandria.payloads import (
    FindNodes,
    Ping,
    Advertise,
    Locate,
    Retrieve,
    GraphGetIntroduction,
    GraphGetNode,
    GraphInsert,
    GraphDelete,
)
from alexandria.routing_table import compute_distance
from alexandria.skip_graph import (
    GraphDB,
    NetworkGraph,
    LocalGraph,
    AlreadyPresent,
    Missing,
    TraversalResults,
    get_traversal_result,
    SGNode,
)
from alexandria.time_queue import TimeQueue
from alexandria.typing import Key, NodeID


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
        self.advertise_queue: TimeQueue[bytes] = TimeQueue(KADEMLIA_ANNOUNCE_INTERVAL)

        (
            self._inbound_content_send_channel,
            self._inbound_content_receive_channel,
        ) = trio.open_memory_channel[Tuple[Node, bytes]](256)

        self.graph_db = GraphDB()
        self._network_graph_ready = trio.Event()

    async def run(self) -> None:
        self.manager.run_task(self._do_initial_content_database_load)

        self.manager.run_daemon_task(self._periodic_report_routing_table_status)

        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._handle_find_nodes_requests)

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
        self.manager.run_daemon_task(self._handle_introduction_requests)
        self.manager.run_daemon_task(self._handle_get_graph_node_requests)
        self.manager.run_daemon_task(self._handle_insert_requests)
        self.manager.run_daemon_task(self._handle_delete_requests)

        self.manager.run_daemon_task(self._monitor_graph)

        await self.manager.wait_finished()

    async def wait_graph_initialized(self) -> None:
        await self._network_graph_ready.wait()

    async def _monitor_graph(self) -> None:
        await self.wait_graph_initialized()

        async for _ in every(60):
            if isinstance(self.graph, LocalGraph):
                self.logger.info("Graph not initialized yet, waiting...")
                continue

            graph_nodes = []
            try:
                async for node in self.graph.iter_items():
                    graph_nodes.append(node)
            except Missing:
                did_error = True
            else:
                did_error = False

            if len(graph_nodes):
                far_left = graph_nodes[0]
                far_right = graph_nodes[-1]
            else:
                far_left = None
                far_right = None
            self.logger.info(
                "Graph traversal %s: found %d nodes: first=%s  last=%s",
                'failed' if did_error else 'finished',
                len(graph_nodes),
                far_left,
                far_right,
            )

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
        async def do_get_introduction(node: Node,
                                      send_channel: trio.abc.SendChannel[Tuple[SGNodeAPI, TraversalResults]],  # noqa: E501
                                      ) -> None:
            try:
                with trio.fail_after(INTRODUCTION_TIMEOUT):
                    candidates = await self.network.get_introduction(node)
            except trio.TooSlowError:
                self.logger.error("Timeout getting introduction")
                return
            self.logger.error("Got %d introductions", len(candidates))

            async with send_channel:
                for candidate in candidates:
                    self.logger.error("Starting Traversal....")
                    import time
                    start_at = time.monotonic()
                    result = await get_traversal_result(
                        self.network,
                        candidate,
                        max_traversal_distance=10,
                    )
                    end_at = time.monotonic()
                    self.logger.error("Finished Traversal: %s seconds", end_at - start_at)
                    await send_channel.send(result)

        while self.manager.is_running:
            random_content_id = NodeID(secrets.randbits(256))
            candidates = await self.network.iterative_lookup(random_content_id)
            if not candidates:
                self.logger.debug("No candidates for introduction")
                await trio.sleep(5)
                continue

            send_channel, receive_channel = trio.open_memory_channel[Tuple[SGNodeAPI, TraversalResults]](0)  # noqa: E501

            async with trio.open_nursery() as nursery:
                async with send_channel:
                    for node in candidates:
                        nursery.start_soon(do_get_introduction, node, send_channel.clone())

                async with receive_channel:
                    introduction_results = tuple([result async for result in receive_channel])

            if not introduction_results:
                self.logger.info("Got no valid introductions")
                await trio.sleep(5)
                continue

            best_result = sorted(
                introduction_results,
                key=lambda result: result.score,
                reverse=True
            )[0]

            if best_result.score > 0.0:
                self.logger.info(
                    "Network SkipGraph initialized: node=%s  score=%s",
                    best_result.node,
                    best_result.score,
                )
                return NetworkGraph(self.graph_db, best_result.node, self.network)
            else:
                self.logger.info(
                    "Failed to initialize Skip Graph. All introductions were faulty: %s",
                    tuple(str(result) for result in introduction_results),
                )

    async def _initialize_skip_graph(self) -> None:
        while self.manager.is_running:
            try:
                with trio.fail_after(2 * INTRODUCTION_TIMEOUT):
                    # 1. Get an introduction to the network's SkipGraph
                    self.graph = await self._initialize_network_graph()
                break
            except trio.TooSlowError:
                if self.config.can_initialize_network_skip_graph:
                    # Use a probabalistic mechanism here so that multiple
                    # bootnodes coming online at the same time are unlikely to
                    # try and concurrently seed the network with content.
                    if True or secrets.randbits(256) >= self.client.local_node_id:
                        self.logger.info("Seeding network graph")
                        zero_node = SGNode(0)
                        self.graph_db.set(0, zero_node)
                        self.graph = NetworkGraph(self.graph_db, zero_node, self.network)
                        break

        # 2. Signal that the network graph is ready
        self._network_graph_ready.set()
        self.logger.info("Network graph set as ready")

    #
    # Request Handling: Skip Graph
    #
    async def _handle_introduction_requests(self) -> None:
        async with self.client.message_dispatcher.subscribe(GraphGetIntroduction) as subscription:
            async for request in subscription:
                self.logger.debug("handling request: %s", request)
                if not self._network_graph_ready.is_set():
                    self.logger.error("%s: Sending empty introduction", request)
                    await self.client.send_graph_introduction(
                        request.node,
                        request_id=request.payload.request_id,
                        graph_nodes=tuple(),
                    )
                else:
                    self.logger.error("%s: Sending introduction: %s", request, self.graph.cursor)
                    await self.client.send_graph_introduction(
                        request.node,
                        request_id=request.payload.request_id,
                        graph_nodes=(self.graph.cursor,),
                    )

    async def _handle_insert_requests(self) -> None:
        async with self.client.message_dispatcher.subscribe(GraphInsert) as subscription:
            async for request in subscription:
                self.logger.debug("handling request: %s", request)
                # Acknowledge the request first, then do the linking.
                await self.client.send_graph_inserted(
                    request.node,
                    request_id=request.payload.request_id,
                )
                if not self._network_graph_ready.is_set():
                    continue

                key = content_key_to_graph_key(request.payload.key)
                try:
                    await self.graph.insert(key)
                except AlreadyPresent:
                    pass
                except Exception:
                    self.logger.exception("Error inserting key: %s", key)
                    continue

    async def _handle_delete_requests(self) -> None:
        async with self.client.message_dispatcher.subscribe(GraphDelete) as subscription:
            async for request in subscription:
                self.logger.debug("handling request: %s", request)
                # Acknowledge the request first, then do the linking.
                await self.client.send_graph_deleted(
                    request.node,
                    request_id=request.payload.request_id,
                )
                if not self._network_graph_ready.is_set():
                    continue

                key = content_key_to_graph_key(request.payload.key)
                try:
                    await self.graph.delete(key)
                except Exception:
                    self.logger.exception("Error deleteing key: %s", key)
                    continue

    async def _handle_get_graph_node_requests(self) -> None:
        async with self.client.message_dispatcher.subscribe(GraphGetNode) as subscription:
            async for request in subscription:
                self.logger.debug("handling request: %s", request)
                key = content_key_to_graph_key(request.payload.key)

                sg_node: Optional[SGNodeAPI]
                try:
                    sg_node = self.graph_db.get(key)
                except KeyError:
                    self.logger.info("Unknown key: %s", key)
                    sg_node = None

                await self.client.send_graph_node(
                    request.node,
                    request_id=request.payload.request_id,
                    sg_node=sg_node,
                )

    #
    # Request Handling: Content
    #
    async def _handle_find_nodes_requests(self) -> None:
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
    def _get_nodes_at_distance(self, distance: int) -> Iterator[Node]:
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

    async def _do_initial_content_database_load(self) -> None:
        self.logger.info("Starting load of all content database into announce queue")
        for key in tuple(self.content_manager.iter_content_keys()):
            await trio.hazmat.checkpoint()
            self.advertise_queue.enqueue(key, 0.0)
        self.logger.info("Finished load of all content database into announce queue")

    async def _periodically_advertise_content(self) -> None:
        async def do_graph_insert(key: Key) -> None:
            # Insert the key into the skip graph.
            with trio.move_on_after(INSERT_TIMEOUT):

                try:
                    sg_node = await self.graph.insert(key)
                except AlreadyPresent:
                    pass
                else:
                    self.graph_db.set(key, sg_node)

            with trio.move_on_after(2 * INSERT_TIMEOUT):
                await self.network.insert(key)

        async def do_announce(key: bytes) -> None:
            with trio.move_on_after(ANNOUNCE_TIMEOUT):
                await self.network.announce(
                    key,
                    Node(self.client.local_node_id, self.client.external_endpoint),
                )

        async with trio.open_nursery() as nursery:
            await self.wait_graph_initialized()

            while self.manager.is_running:
                key = await self.advertise_queue.pop_next()
                if not self.content_manager.has_data(key):
                    self.logger.debug(
                        'Discarding announcement key missing content: %s',
                        encode_hex(key),
                    )
                    continue

                self.logger.debug('Announcing: %s', encode_hex(key))

                nursery.start_soon(do_announce, key)
                nursery.start_soon(do_graph_insert, content_key_to_graph_key(key))

                self.advertise_queue.enqueue(key)

    async def _ingest_content(self, node: Node, key: bytes) -> None:
        data: Optional[bytes]
        if self._check_interest_in_ephemeral_content(key):
            try:
                with trio.fail_after(5):
                    try:
                        data = await self.network.retrieve(node, key=key)
                    except ContentNotFound:
                        self.logger.debug(
                            'Content retrieval timed out: %s@%s',
                            encode_hex(key),
                            node,
                        )
                        data = None
                    else:
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
            self.advertise_queue.enqueue(key, queue_at=0.0)

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
