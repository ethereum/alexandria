import collections
import ipaddress
import itertools
import logging
import math
import secrets
import socket
from typing import AsyncIterable, Generator, Iterable, Sequence, Tuple

from async_service import Service
from eth_utils import to_tuple
from eth_utils.toolz import partition_all, take
import trio

from alexandria._utils import every, humanize_node_id
from alexandria.abc import (
    Endpoint,
    EndpointDatabaseAPI,
    MessageDispatcherAPI,
    Node,
    RoutingTableAPI,
)
from alexandria.constants import FOUND_NODES_PAYLOAD_SIZE
from alexandria.messages import FindNodes, FoundNodes, Ping, Pong, Message
from alexandria.routing_table import compute_distance, compute_log_distance
from alexandria.typing import NodeID


ROUTING_TABLE_PING_INTERVAL = 5  # interval of outgoing pings sent to maintain the routing table
ROUTING_TABLE_LOOKUP_INTERVAL = 5  # intervals between lookups

NODES_PER_PAYLOAD = FOUND_NODES_PAYLOAD_SIZE / 38

LOOKUP_CONCURRENCY_FACTOR = 3  # maximum number of concurrent FindNodes lookups
LOOKUP_RETRY_THRESHOLD = 5  # minimum number of ENRs desired in responses to FindNode requests


NodePayload = Tuple[NodeID, bytes, int]


class _EmptyFindNodesResponse(Exception):
    pass


class RoutingTableManager(Service):
    logger = logging.getLogger('alexandria.routing_table_manager.RoutingTableManager')

    def __init__(self,
                 routing_table: RoutingTableAPI,
                 endpoint_db: EndpointDatabaseAPI,
                 message_dispatcher: MessageDispatcherAPI,
                 local_endpoint: Endpoint,
                 ) -> None:
        self.endpoint_db = endpoint_db
        self.routing_table = routing_table
        self.message_dispatcher = message_dispatcher
        self.local_endpoint = local_endpoint

    async def run(self) -> None:
        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._ping_occasionally)
        self.manager.run_daemon_task(self._lookup_occasionally)
        self.manager.run_daemon_task(self._handle_lookup)

    async def _handle_lookup(self) -> None:
        with self.message_dispatcher.subscribe(FindNodes) as subscription:
            async for request in subscription.stream():
                if request.payload.distance == 0:
                    local_node_id = self.routing_table.center_node_id
                    local_endpoint = self.local_endpoint
                    nodes = (
                        (local_node_id, socket.inet_aton(local_endpoint[0]), local_endpoint[1]),
                    )
                else:
                    nodes = self._get_nodes_at_distance(request.payload.distance)

                batches = tuple(partition_all(NODES_PER_PAYLOAD, nodes))
                async for batch in batches:
                    response = Message(
                        FoundNodes(request.payload.request_id, len(batches), batch),
                        request.node_id,
                        request.endpoint,
                    )
                    await self.message_dispatcher.send_message(response)

    @to_tuple
    def _get_nodes_at_distance(self, distance: int) -> None:
        """Send a Nodes message containing ENRs of peers at a given node distance."""
        nodes_at_distance = self.routing_table.get_nodes_at_log_distance(distance)

        for node_id in nodes_at_distance:
            try:
                endpoint = self.endpoint_db.get_endpoint(node_id)
            except KeyError:
                self.logger.error('Failed to lookup endpoint for: %s', hex(node_id))
                self.routing_table.remove(node_id)
                continue

            yield (
                node_id,
                socket.inet_aton(endpoint.ip_address),
                endpoint.port,
            )

    async def _lookup_occasionally(self) -> None:
        async with trio.open_nursery() as nursery:
            async for _ in every(ROUTING_TABLE_LOOKUP_INTERVAL):  # noqa: F841
                if self.routing_table.is_empty:
                    self.logger.debug('Aborting scheduled lookup due to empty routing table')
                    continue

                async def _ensure_node_reachable(node):
                    # first establish that we can communicate with them...
                    with trio.move_on_after(2):
                        await self.message_dispatcher.request(Message(
                            Ping(self.message_dispatcher.get_free_request_id(node.node_id)),
                            node.node_id,
                            node.endpoint,
                        ), Pong)
                        self.routing_table.update(node.node_id)
                        self.endpoint_db.set_endpoint(node.node_id, node.endpoint)
                        self.logger.debug(
                            'Established connection with found node: %s@%s',
                            humanize_node_id(node.node_id),
                            node.endpoint,
                        )

                target_node_id = NodeID(secrets.randbits(256))
                found_nodes = await self.lookup(target_node_id)
                self.logger.debug(
                    'Lookup for %s yielded %d nodes',
                    humanize_node_id(target_node_id),
                    len(found_nodes),
                )
                for node in found_nodes:
                    nursery.start_soon(_ensure_node_reachable, node)

    async def lookup(self, target_id: NodeID) -> Tuple[Node, ...]:
        self.logger.info("Looking up %s", humanize_node_id(target_id))

        queried_node_ids = set()
        unresponsive_node_ids = set()
        received_nodes: DefaultDict[NodeID, Set[Endpoint]] = collections.defaultdict(set)

        async def lookup_and_store_response(peer: Node) -> None:
            found_nodes = 0

            with trio.move_on_after(5):
                async for node in self.lookup_at_peer(peer, target_id):
                    found_nodes += 1
                    received_nodes[node.node_id].append(node.endpoint)

            if found_nodes == 0:
                unresponsive_node_ids.add(peer.node_id)

        @to_tuple
        def get_endpoints(node_id: NodeID) -> Iterable[Endpoint]:
            try:
                yield self.endpoint_db.get_endpoint(node_id)
            except KeyError:
                pass

            yield from received_nodes[node_id]

        for lookup_round_counter in itertools.count():
            received_node_ids = tuple(received_nodes.keys())
            candidates = iter_closest_nodes(target_id, self.routing_table, received_node_ids)
            responsive_candidates = itertools.dropwhile(
                lambda node: node in unresponsive_node_ids,
                candidates,
            )
            closest_k_candidates = take(self.routing_table.bucket_size, responsive_candidates)
            closest_k_unqueried_candidates = (
                candidate
                for candidate in closest_k_candidates
                if candidate not in queried_node_ids
            )
            nodes_to_query = tuple(take(
                LOOKUP_CONCURRENCY_FACTOR,
                closest_k_unqueried_candidates,
            ))

            if nodes_to_query:
                self.logger.debug(
                    "Starting lookup round %d for %s",
                    lookup_round_counter + 1,
                    humanize_node_id(target_id),
                )
                async with trio.open_nursery() as nursery:
                    for peer_id in nodes_to_query:
                        for endpoint in get_endpoints(peer_id):
                            nursery.start_soon(lookup_and_store_response, Node(peer_id, endpoint))
            else:
                self.logger.debug(
                    "Lookup for %s finished in %d rounds",
                    humanize_node_id(target_id),
                    lookup_round_counter,
                )
                break

        return tuple(
            Node(node_id, endpoint)
            for node_id, endpoints in received_nodes.items()
            for endpoint in endpoints
        )

    async def lookup_at_peer(self, peer: Node, target_id: NodeID) -> AsyncIterable[Node]:
        self.logger.debug(
            "Looking up %s via node %s",
            humanize_node_id(target_id),
            humanize_node_id(peer.node_id),
        )
        distance = compute_log_distance(peer.node_id, target_id)

        request = Message(
            FindNodes(self.message_dispatcher.get_free_request_id(peer.node_id), distance),
            peer.node_id,
            peer.endpoint,
        )
        response_count = 0
        with self.message_dispatcher.request_response(request, FindNodes) as subscription:
            async for response in subscription.stream():
                response_count += 1
                for node_id, ip_address, port in response.payload.nodes:
                    endpoint = Endpoint(ipaddress.IPv4Address(ip_address), port)
                    node = Node(node_id, endpoint)
                    yield node

                if response.total == response_count:
                    break

    async def _ping_occasionally(self) -> None:
        async for _ in every(ROUTING_TABLE_PING_INTERVAL):  # noqa: F841
            if not self.routing_table.is_empty:
                log_distance = self.routing_table.get_least_recently_updated_log_distance()
                candidates = self.routing_table.get_nodes_at_log_distance(log_distance)
                for node_id in reversed(candidates):
                    endpoint = self.endpoint_db.get_endpoint(node_id)

                    message = Message(
                        Ping(self.message_dispatcher.get_free_request_id(node_id)),
                        node_id,
                        endpoint,
                    )
                    self.logger.debug("Pinging %s", humanize_node_id(node_id))
                    with trio.move_on_after(10) as scope:
                        await self.message_dispatcher.request(message, Pong)

                    if scope.cancelled_caught:
                        self.routing_table.remove(node_id)
                    else:
                        break
            else:
                self.logger.warning("Routing table is empty, no one to ping")

    async def _pong_when_pinged(self) -> None:
        with self.message_dispatcher.subscribe(Ping) as subscription:
            async for message in subscription.stream():
                self.logger.debug(
                    "Got ping from %s, responding with pong",
                    humanize_node_id(message.node_id),
                )
                response = Message(
                    Pong(message.payload.request_id),
                    message.node_id,
                    message.endpoint,
                )
                await self.message_dispatcher.send_message(response)


def iter_closest_nodes(target: NodeID,
                       routing_table: RoutingTableAPI,
                       seen_nodes: Sequence[NodeID],
                       ) -> Generator[NodeID, None, None]:
    """Iterate over the nodes in the routing table as well as additional nodes in order of
    distance to the target. Duplicates will only be yielded once.
    """
    def dist(node: NodeID) -> float:
        if node is not None:
            return compute_distance(target, node)
        else:
            return math.inf

    yielded_nodes: Set[NodeID] = set()
    routing_iter = routing_table.iter_nodes_around(target)
    seen_iter = iter(sorted(seen_nodes, key=dist))
    closest_routing = next(routing_iter, None)
    closest_seen = next(seen_iter, None)

    while not (closest_routing is None and closest_seen is None):
        if dist(closest_routing) < dist(closest_seen):
            node_to_yield = closest_routing
            closest_routing = next(routing_iter, None)
        else:
            node_to_yield = closest_seen
            closest_seen = next(seen_iter, None)

        if node_to_yield not in yielded_nodes:
            yielded_nodes.add(node_to_yield)
            yield node_to_yield
