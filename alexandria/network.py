import collections
import itertools
import logging
import math
from typing import DefaultDict, Iterable, Optional, Sequence, Set, Tuple

from eth_utils import to_tuple
from eth_utils.toolz import take
import trio

from alexandria._utils import humanize_node_id
from alexandria.abc import (
    ClientAPI,
    Endpoint,
    EndpointDatabaseAPI,
    NetworkAPI,
    Node,
    RoutingTableAPI,
)
from alexandria.constants import FIND_NODES_TIMEOUT
from alexandria.routing_table import compute_log_distance, compute_distance
from alexandria.typing import NodeID


LOOKUP_CONCURRENCY_FACTOR = 3  # maximum number of concurrent FindNodes lookups


class Network(NetworkAPI):
    logger = logging.getLogger('alexandria.network.Network')

    def __init__(self,
                 client: ClientAPI,
                 endpoint_db: EndpointDatabaseAPI,
                 routing_table: RoutingTableAPI,
                 ) -> None:
        self.client = client
        self.endpoint_db = endpoint_db
        self.routing_table = routing_table

    async def single_lookup(self, node: Node, distance: int) -> Tuple[Node, ...]:
        found_nodes = await self.client.find_nodes(node, distance=distance)
        return tuple(
            Node.from_payload(node_as_payload)
            for message in found_nodes
            for node_as_payload in message.payload.nodes
        )

    async def iterative_lookup(self, target_id: NodeID) -> Tuple[Node, ...]:
        self.logger.info("Starting looking up @ %s", humanize_node_id(target_id))

        # tracks the nodes that have already been queried
        queried_node_ids: Set[NodeID] = set()
        # keeps track of the nodes that are unresponsive
        unresponsive_node_ids: Set[NodeID] = set()
        # accumulator of all of the valid responses received
        received_nodes: DefaultDict[NodeID, Set[Endpoint]] = collections.defaultdict(set)

        async def do_lookup(peer: Node) -> None:
            self.logger.debug(
                "Looking up %s via node %s",
                humanize_node_id(target_id),
                humanize_node_id(peer.node_id),
            )
            distance = compute_log_distance(peer.node_id, target_id)

            try:
                with trio.fail_after(FIND_NODES_TIMEOUT):
                    found_nodes = await self.single_lookup(
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

        for lookup_round_number in itertools.count():
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
                    lookup_round_number + 1,
                    humanize_node_id(target_id),
                )
                queried_node_ids.update(nodes_to_query)
                async with trio.open_nursery() as nursery:
                    for peer_id in nodes_to_query:
                        for endpoint in get_endpoints(peer_id):
                            nursery.start_soon(do_lookup, Node(peer_id, endpoint))
            else:
                self.logger.debug(
                    "Lookup for %s finished in %d rounds",
                    humanize_node_id(target_id),
                    lookup_round_number,
                )
                break

        found_nodes = tuple(
            Node(node_id, endpoint)
            for node_id, endpoints in received_nodes.items()
            for endpoint in endpoints
        )
        self.logger.info(
            "Finished looking up %s in %d rounds: Found %d nodes after querying %d nodes",
            humanize_node_id(target_id),
            lookup_round_number,
            len(found_nodes),
            len(queried_node_ids),
        )
        return found_nodes

    async def verify_and_add(self, node: Node) -> None:
        """
        Verify we can ping a node and then add it to our routing table and
        endpoint database.
        """
        await self.client.ping(node)
        self.endpoint_db.set_endpoint(node.node_id, node.endpoint)
        self.routing_table.update(node.node_id)

    async def locate(self, node: Node, *, key: bytes) -> Tuple[Node, ...]:
        locations = await self.client.locate(node, key=key)
        return tuple(
            Node.from_payload(node_payload)
            for message in locations
            for node_payload in message.payload.nodes
        )

    async def retrieve(self, node: Node, *, key: bytes) -> bytes:
        chunks = await self.client.retrieve(node, key=key)
        data = b''.join((
            message.payload.data for message in chunks
        ))
        return data

    async def bond(self, node: Node) -> None:
        """
        Establish a session with the given node if one is not already present.

        1. Ensure we can communicate with the node
        2. Perform a lookup on the node of our own location in the neighborhood
           to find the nodes that are closest to our node id.
        """
        self.logger.debug('Initiating bond with: %s', node)

        distance = compute_log_distance(self.client.local_node_id, node.node_id)

        found_nodes = await self.single_lookup(node, distance=distance)

        self.endpoint_db.set_endpoint(node.node_id, node.endpoint)
        self.routing_table.update(node.node_id)

        async with trio.open_nursery() as nursery:
            for neighbor in found_nodes:
                nursery.start_soon(self.verify_and_add, neighbor)


def iter_closest_nodes(target: NodeID,
                       routing_table: RoutingTableAPI,
                       seen_nodes: Sequence[NodeID],
                       ) -> Iterable[NodeID]:
    """Iterate over the nodes in the routing table as well as additional nodes in order of
    distance to the target. Duplicates will only be yielded once.
    """
    def dist(node: Optional[NodeID]) -> float:
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

        if node_to_yield is None:
            raise Exception("Invariant")

        if node_to_yield not in yielded_nodes:
            yielded_nodes.add(node_to_yield)
            yield node_to_yield
