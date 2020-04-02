import logging
from typing import Collection

from async_service import Service
from eth_keys import keys
import trio

from alexandria.abc import ClientAPI, Endpoint, EndpointDatabaseAPI, Node, RoutingTableAPI
from alexandria.client import Client
from alexandria.endpoint_db import MemoryEndpointDB
from alexandria.messages import Message, Ping, Pong
from alexandria.routing_table import RoutingTable
from alexandria.routing_table_manager import RoutingTableManager


BOND_TIMEOUT = 10


class Application(Service):
    logger = logging.getLogger('alexandria.app.Application')

    client: ClientAPI
    endpoint_db: EndpointDatabaseAPI
    routing_table: RoutingTableAPI

    def __init__(self,
                 bootnodes: Collection[Node],
                 private_key: keys.PrivateKey,
                 listen_on: Endpoint,
                 ) -> None:
        self.client = Client(
            private_key=private_key,
            listen_on=listen_on,
        )
        self.bootnodes = bootnodes
        self.endpoint_db = MemoryEndpointDB()
        self.routing_table = RoutingTable(
            self.client.local_node_id,
            bucket_size=256,
        )
        self._routing_table_manager = RoutingTableManager(
            routing_table=self.routing_table,
            endpoint_db=self.endpoint_db,
            message_dispatcher=self.client.message_dispatcher,
            local_endpoint=self.client.listen_on,
        )

    async def run(self) -> None:
        with trio.fail_after(5):
            async with self.client.events.listening.subscribe():
                self.manager.run_daemon_child_service(self.client)

        await trio.sleep(0.1)

        self.manager.run_task(self._bootstrap)
        self.manager.run_daemon_child_service(self._routing_table_manager)

        await self.manager.wait_finished()

    async def _monitor_endpoints(self) -> None:
        async with self.client.events.handshake_complete.subscribe() as subscription:
            async for session in subscription:
                self.endpoint_db.set_endpoint(session.remote_node_id, session.remote_endpoint)

    async def _bond(self, node: Node) -> None:
        if self.client.pool.has_session(node.node_id):
            self.logger.debug('Skipping bond. Session already active with %s', node)
        else:
            self.logger.debug('Initiating bond with: %s', node)

        with trio.move_on_after(BOND_TIMEOUT) as scope:
            message = Message(
                Ping(request_id=self.client.message_dispatcher.get_free_request_id(node.node_id)),
                node_id=node.node_id,
                endpoint=node.endpoint,
            )
            await self.client.message_dispatcher.request(message, Pong)
            self.endpoint_db.set_endpoint(node.node_id, node.endpoint)
            self.routing_table.update(node.node_id)
            self.logger.info('Bonded successfully with: %s', node)

        if scope.cancelled_caught:
            self.logger.debug('Timed out bonding with: %s', node)

    async def _bootstrap(self) -> None:
        self.logger.info("Attempting to bond with %d bootnodes", len(self.bootnodes))

        async with trio.open_nursery() as nursery:
            for enode in self.bootnodes:
                nursery.start_soon(self._bond, enode)
