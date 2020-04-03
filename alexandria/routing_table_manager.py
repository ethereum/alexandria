import collections
import itertools
import logging
import math
import secrets
from typing import Iterable, Sequence, Tuple

from async_service import Service
from eth_utils import to_tuple
from eth_utils.toolz import take
import trio

from alexandria._utils import every, humanize_node_id
from alexandria.abc import (
    ClientAPI,
    Endpoint,
    EndpointDatabaseAPI,
    Node,
    RoutingTableAPI,
)
from alexandria.constants import FOUND_NODES_PAYLOAD_SIZE
from alexandria.payloads import FindNodes, Ping
from alexandria.routing_table import compute_distance, compute_log_distance
from alexandria.typing import NodeID


ROUTING_TABLE_PING_INTERVAL = 30  # interval of outgoing pings sent to maintain the routing table
ROUTING_TABLE_LOOKUP_INTERVAL = 10  # intervals between lookups

NODES_PER_PAYLOAD = FOUND_NODES_PAYLOAD_SIZE // 38

LOOKUP_CONCURRENCY_FACTOR = 3  # maximum number of concurrent FindNodes lookups
LOOKUP_RETRY_THRESHOLD = 5  # minimum number of ENRs desired in responses to FindNode requests

PING_TIMEOUT = 3


NodePayload = Tuple[NodeID, bytes, int]


class _EmptyFindNodesResponse(Exception):
    pass


class RoutingTableManager(Service):
    logger = logging.getLogger('alexandria.routing_table_manager.RoutingTableManager')

    def __init__(self,
                 routing_table: RoutingTableAPI,
                 endpoint_db: EndpointDatabaseAPI,
                 client: ClientAPI,
                 ) -> None:
        self.endpoint_db = endpoint_db
        self.routing_table = routing_table
        self.client = client

    async def run(self) -> None:
        self.manager.run_task(self._do_initial_routing_table_population)
        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._ping_occasionally)
        self.manager.run_daemon_task(self._lookup_occasionally)
        self.manager.run_daemon_task(self._handle_lookup)
        await self.manager.wait_finished()

    async def _do_initial_routing_table_population(self) -> None:
        self.logger.info('Doing initial routing table population...')
        while self.routing_table.is_empty:
            self.logger.info('Routing table empty.  Waiting....')
            await trio.sleep(1)

        async with trio.open_nursery() as nursery:
            async def do_lookup(peer, distance):
                found_nodes = await self.lookup_from_peer(peer, distance)

                for node in found_nodes:
                    is_reachable = await self._verify_node(node)
                    if is_reachable:
                        self.logger.info('Found reachable node: %s', node)
                    else:
                        self.logger.info('Found unreachable node: %s', node)

            for node_id in self.routing_table.iter_nodes():
                endpoint = self.endpoint_db.get_endpoint(node_id)
                peer = Node(node_id, endpoint)
                for distance in range(1, 256):
                    nursery.start_soon(do_lookup, peer, distance)

    async def _handle_lookup(self) -> None:
        with self.client.message_dispatcher.subscribe(FindNodes) as subscription:
            async for request in subscription.stream():
                self.logger.debug("handling request: %s", request)

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
            try:
                endpoint = self.endpoint_db.get_endpoint(node_id)
            except KeyError:
                self.logger.error('Failed to lookup endpoint for: %s', hex(node_id))
                self.routing_table.remove(node_id)
                continue

            yield Node(node_id, endpoint)

    async def _verify_node(self, node: Node) -> bool:
        with trio.move_on_after(PING_TIMEOUT) as scope:
            await self.client.ping(node)

        if scope.cancelled_caught:
            return False

        self.routing_table.update(node.node_id)
        self.endpoint_db.set_endpoint(node.node_id, node.endpoint)
        self.logger.debug('verified node: %s@%s', node)
        return True

    async def _lookup_occasionally(self) -> None:
        async with trio.open_nursery() as nursery:
            async for _ in every(ROUTING_TABLE_LOOKUP_INTERVAL):  # noqa: F841
                if self.routing_table.is_empty:
                    self.logger.debug('Aborting scheduled lookup due to empty routing table')
                    continue

                target_node_id = NodeID(secrets.randbits(256))
                found_nodes = await self.lookup(target_node_id)
                self.logger.debug(
                    'Lookup for %s yielded %d nodes',
                    humanize_node_id(target_node_id),
                    len(found_nodes),
                )
                for node in found_nodes:
                    nursery.start_soon(self._verify_node, node)

    async def lookup(self, target_id: NodeID) -> Tuple[Node, ...]:
        self.logger.info("Starting looking up @ %s", humanize_node_id(target_id))

        queried_node_ids = set()
        unresponsive_node_ids = set()
        received_nodes: DefaultDict[NodeID, Set[Endpoint]] = collections.defaultdict(set)

        async def do_lookup(peer: Node) -> None:
            self.logger.debug(
                "Looking up %s via node %s",
                humanize_node_id(target_id),
                humanize_node_id(peer.node_id),
            )
            distance = compute_log_distance(peer.node_id, target_id)

            try:
                with trio.fail_after(5):
                    found_nodes = await self.client.find_nodes(
                        peer,
                        distance=distance,
                    )
            except trio.TooSlowError:
                unresponsive_node_ids.add(peer.node_id)
            else:
                if len(found_nodes) == 0:
                    unresponsive_node_ids.add(peer.node_id)
                else:
                    for node in found_nodes:
                        received_nodes[node.node_id].add(node.endpoint)

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
                            nursery.start_soon(do_lookup, Node(peer_id, endpoint))
            else:
                self.logger.debug(
                    "Lookup for %s finished in %d rounds",
                    humanize_node_id(target_id),
                    lookup_round_counter,
                )
                break

        found_nodes = tuple(
            Node(node_id, endpoint)
            for node_id, endpoints in received_nodes.items()
            for endpoint in endpoints
        )
        self.logger.info(
            "Finished looking up @ %s: Found %d nodes",
            humanize_node_id(target_id),
            len(found_nodes),
        )
        return found_nodes

    async def _ping_occasionally(self) -> None:
        async for _ in every(ROUTING_TABLE_PING_INTERVAL):  # noqa: F841
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
            async for message in subscription.stream():
                self.logger.debug(
                    "Got ping from %s, responding with pong",
                    humanize_node_id(message.node_id),
                )
                await self.client.send_pong(message.node, request_id=message.payload.request_id)


def iter_closest_nodes(target: NodeID,
                       routing_table: RoutingTableAPI,
                       seen_nodes: Sequence[NodeID],
                       ) -> Iterable[NodeID]:
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
