import collections
import functools
import logging
import secrets
from typing import DefaultDict, Iterable, Mapping, NamedTuple, Set, Tuple

from async_service import Service
from eth_utils import to_tuple
from eth_utils.toolz import partition_all
import trio

from alexandria._utils import every, humanize_node_id, content_key_to_node_id
from alexandria.abc import (
    ClientAPI,
    EndpointDatabaseAPI,
    NetworkAPI,
    Node,
    RoutingTableAPI,
)
from alexandria.constants import PING_TIMEOUT
from alexandria.payloads import FindNodes, Ping, Advertise, Locate, Retrieve
from alexandria.typing import NodeID


KADEMLIA_PING_INTERVAL = 30  # interval of outgoing pings sent to maintain the routing table
KADEMLIA_LOOKUP_INTERVAL = 60  # intervals between lookups
KADEMLIA_ANNOUNCE_INTERVAL = 600  # 10 minutes
KADEMLIA_ANNOUNCE_CONCURRENCY = 3


NodePayload = Tuple[NodeID, bytes, int]


class _EmptyFindNodesResponse(Exception):
    pass


class KademliaConfig(NamedTuple):
    LOOKUP_INTERVAL: int = KADEMLIA_LOOKUP_INTERVAL
    PING_INTERVAL: int = KADEMLIA_PING_INTERVAL
    ANNOUNCE_INTERVAL: int = KADEMLIA_ANNOUNCE_INTERVAL
    ANNOUNCE_CONCURRENCY: int = KADEMLIA_ANNOUNCE_CONCURRENCY


class Kademlia(Service):
    logger = logging.getLogger('alexandria.kademlia.Kademlia')
    content_index: DefaultDict[bytes, Set[NodeID]]

    def __init__(self,
                 routing_table: RoutingTableAPI,
                 endpoint_db: EndpointDatabaseAPI,
                 content_db: Mapping[bytes, bytes],
                 client: ClientAPI,
                 network: NetworkAPI,
                 config: KademliaConfig = None,
                 ) -> None:
        self.content_db = content_db
        self.content_index = collections.defaultdict(set)
        self.endpoint_db = endpoint_db
        self.routing_table = routing_table
        self.client = client
        self.network = network

        if config is None:
            config = KademliaConfig()
        self.config = config

        # populate the index
        for key in self.content_db:
            self.content_index[key].add(self.client.local_node_id)

    async def run(self) -> None:
        self.manager.run_daemon_task(self._periodic_report_routing_table_status)

        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._handle_lookup_requests)

        self.manager.run_daemon_task(self._handle_advertise_requests)
        self.manager.run_daemon_task(self._handle_locate_requests)
        self.manager.run_daemon_task(self._handle_retrieve_requests)

        self.manager.run_daemon_task(self._periodically_announce_content)

        self.manager.run_daemon_task(self._ping_occasionally)
        self.manager.run_daemon_task(self._lookup_occasionally)

        await self.manager.wait_finished()

    async def _periodic_report_routing_table_status(self) -> None:
        async for _ in every(30, 10):  # noqa: F841
            stats = self.routing_table.get_stats()
            if stats.full_buckets:
                full_buckets = '/'.join((str(index) for index in stats.full_buckets))
            else:
                full_buckets = 'None'
            self.logger.info(
                (
                    "\n"
                    "###################[%s]#####################\n"
                    "       RoutingTable(bucket_size=%d, num_buckets=%d):\n"
                    "         - %d nodes\n"
                    "         - full buckets: %s\n"
                    "         - %d nodes in replacement cache\n"
                    "       ContentDB():\n"
                    "         - content: %d\n"
                    "         - index: %d\n"
                    "####################################################"
                ),
                humanize_node_id(self.client.local_node_id),
                stats.bucket_size,
                stats.num_buckets,
                stats.total_nodes,
                full_buckets,
                stats.num_in_replacement_cache,
                len(self.content_db),
                sum(len(location_keys) for location_keys in self.content_index.values()),
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
        async with trio.open_nursery() as nursery:
            limiter = trio.CapacityLimiter(self.config.ANNOUNCE_CONCURRENCY)

            async def do_announce(node: Node, key: bytes, who: Node) -> None:
                async with limiter:
                    await self.client.send_advertise(node, key=key, who=who)

            async for _ in every(self.config.ANNOUNCE_INTERVAL):  # noqa: F841
                for key, value in tuple(self.content_db.items()):
                    content_node_id = content_key_to_node_id(key)
                    nodes_around = self.routing_table.iter_nodes_around(content_node_id)
                    for batch in partition_all(self.config.ANNOUNCE_CONCURRENCY, nodes_around):
                        for node_id in batch:
                            endpoint = self.endpoint_db.get_endpoint(node_id)
                            node = Node(node_id, endpoint)
                            nursery.start_soon(
                                do_announce,
                                node,
                                key,
                                self.client.local_node,
                            )

    async def _handle_advertise_requests(self) -> None:
        with self.client.message_dispatcher.subscribe(Advertise) as subscription:
            while self.manager.is_running:
                request = await subscription.receive()
                payload = request.payload
                node_id, ip_address, port = payload.node
                # TODO: ping the node to ensure it is available (unless it is the sending node).
                # TODO: verify content is actually available
                # TODO: check distance of key and store conditionally
                self.content_index[payload.key].add(payload.node)
                await self.client.send_ack(request.node, request_id=payload.request_id)

    async def _handle_locate_requests(self) -> None:
        with self.client.message_dispatcher.subscribe(Locate) as subscription:
            while self.manager.is_running:
                request = await subscription.receive()
                payload = request.payload
                # TODO: ping the node to ensure it is available (unless it is the sending node).
                # TODO: verify content is actually available
                # TODO: check distance of key and store conditionally
                location_ids = tuple(self.content_index[payload.key])
                locations = tuple(
                    Node(node_id, self.endpoint_db.get_endpoint(node_id))
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
                    data = self.content_db[payload.key]
                except KeyError:
                    data = b''

                await self.client.send_chunks(
                    request.node,
                    request_id=payload.request_id,
                    data=data,
                )
