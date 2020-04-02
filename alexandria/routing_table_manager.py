import ipaddress
import secrets
import socket
from typing import AsyncIterable, Tuple

from async_service import Service
from eth_utils import to_tuple
from eth_utils.toolz import partition_all
import trio

from alexandria._utils import every, humanize_node_id
from alexandria.abc import Endpoint, MessageDispatcherAPI, RoutingTableAPI, PoolAPI
from alexandria.constants import FOUND_NODES_PAYLOAD_SIZE
from alexandria.exceptions import SessionNotFound
from alexandria.messages import FindNodes, FoundNodes, Ping, Pong, Message
from alexandria.routing_table import compute_distance
from alexandria.typing import NodeID


ROUTING_TABLE_PING_INTERVAL = 30  # interval of outgoing pings sent to maintain the routing table
ROUTING_TABLE_LOOKUP_INTERVAL = 60  # intervals between lookups

NODES_PER_PAYLOAD = FOUND_NODES_PAYLOAD_SIZE / 38

LOOKUP_CONCURRENCY_FACTOR = 3


NodePayload = Tuple[NodeID, bytes, int]


class RoutingTableManager(Service):
    def __init__(self,
                 routing_table: RoutingTableAPI,
                 pool: PoolAPI,
                 message_dispatcher: MessageDispatcherAPI,
                 local_endpoint: Endpoint,
                 ) -> None:
        self.pool = pool,
        self.routing_table = routing_table
        self.message_dispatcher = message_dispatcher

    async def run(self) -> None:
        self.manager.run_daemon_task(self._pong_when_pinged)
        self.manager.run_daemon_task(self._ping_occasionally)
        self.manager.run_daemon_task(self._lookup_occasionally)
        self.manager.run_daemon_task(self._handle_lookup)

    async def _handle_lookup(self) -> None:
        with self.message_dispatcher.subscribe(FindNodes) as subscription:
            async for request in subscription.stream():
                if request.payload.distance == 0:
                    local_node_id = self.pool.local_node_id
                    local_endpoint = self.local_endpoint
                    nodes = (
                        (local_node_id, socket.inet_aton(local_endpoint[0]), local_endpoint[1]),
                    )
                else:
                    nodes = self._get_nodes_at_distance(request.payload.distance)

                # TODO:
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
                session = self.pool.get_session(node_id)
            except SessionNotFound:
                continue
            yield (
                node_id,
                socket.inet_aton(session.remote_endpoint.ip_address),
                session.remote_endpoint.port,
            )

    async def _lookup_occasionally(self) -> None:
        async for _ in every(ROUTING_TABLE_LOOKUP_INTERVAL):  # noqa: F841
            if self.routing_table.is_empty:
                self.logger.debug('Aborting scheduled lookup due to empty routing table')
                continue

            target_node_id = NodeID(secrets.randbits(256))
            nodes = await self.find_nodes(target_node_id)
            for node_id, ip_address, port in nodes:
                endpoint = Endpoint(ipaddress.IPv4Address(ip_address), port)
                # ping them so that we get a record of them in our database?
                await self.message_dispatcher.send_message(Message(
                    Ping(self.message_dispatcher.get_free_request_id(node_id)),
                    node_id,
                    endpoint,
                ))

    async def find_nodes(self, target_node_id: NodeID) -> Tuple[NodePayload, ...]:
        """
        TODO: Needs to continually edge in closer to the target node_id.
        """
        candidates = set(self.routing_table.iter_nodes_around(target_node_id))
        queried = set()
        found = set()

        async def do_lookup(node_id):
            queried.add(node_id)

            with trio.move_on_after(4) as scope:
                async for node in self.lookup_at_peer(node_id, target_node_id):
                    if node not in queried:
                        candidates.add(node[0])
                    found.add(node)
                    self.routing_table.update(node_id)

            if scope.cancelled_caught:
                self.logger.debug('Aborted lookup with node: %s', humanize_node_id(node_id))
            else:
                self.logger.debug('Completed lookup with node: %s', humanize_node_id(node_id))

        self.logger.debug('Starting lookup...')
        with trio.move_on_after(ROUTING_TABLE_LOOKUP_INTERVAL - 5) as scope:
            while candidates:
                for batch in partition_all(LOOKUP_CONCURRENCY_FACTOR, candidates):
                    async with trio.open_nursery() as nursery:
                        for node_id in batch:
                            nursery.start_soon(do_lookup, node_id)
                candidates = candidates.difference(queried)

        if scope.cancelled_caught:
            self.logger.debug('Aborted lookup...')
        else:
            self.logger.debug('Finished lookup...')

        return found

    async def lookup_at_peer(self,
                             node_id: NodeID,
                             target_node_id: NodeID,
                             ) -> AsyncIterable[NodePayload]:
        distance = compute_distance(node_id, target_node_id)
        session = self.pool.get_session(node_id)
        request = Message(
            FindNodes(self.get_free_request_id(node_id), distance),
            node_id,
            session.remote_endpoint,
        )
        with self.message_dispatcher.request_response(request, FoundNodes) as subscription:
            async for response in subscription.stream():
                yield from response.payload.nodes

    async def _ping_occasionally(self) -> None:
        async for _ in every(ROUTING_TABLE_PING_INTERVAL):  # noqa: F841
            if not self.routing_table.is_empty:
                log_distance = self.routing_table.get_least_recently_updated_log_distance()
                candidates = self.routing_table.get_nodes_at_log_distance(log_distance)
                for node_id in reversed(candidates):
                    try:
                        session = self.pool.get_session(node_id)
                    except SessionNotFound:
                        self.routing_table.remove(node_id)
                        continue

                    message = Message(
                        Ping(self.message_dispatcher.get_free_request_id(node_id)),
                        node_id,
                        session.remote_endpoint,
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
