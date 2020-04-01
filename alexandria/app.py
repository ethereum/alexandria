import logging
from typing import Collection

from async_service import Service
from eth_keys import keys
import trio

from alexandria.abc import Endpoint
from alexandria.client import Client
from alexandria.enode import Node
from alexandria.messages import Message, Ping, Pong


BOND_TIMEOUT = 10


class Application(Service):
    logger = logging.getLogger('alexandria.app.Application')

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
        self.routing_table = RoutingTable(
            self.client.local_node_id,
            bucket_size=256,
        )
        self.routing_table_manager = RoutingTableManager(
            ...  # TODO: ...
        )

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.client)
        self.manager.run_daemon_child_service(self.routing_table_manager)
        self.manager.run_task(self._bootstrap)

    async def _bond(self, node: Node) -> None:
        if self.client.pool.has_session(node.node_id):
            self.logger.debug('Skipping bond. Session already active with %s', node)
        else:
            self.logger.debug('Initiating bond with: %s', node)

        async with trio.move_on_after(BOND_TIMEOUT) as scope:
            message = Message(
                Ping(request_id=self.client.get_free_request_id()),
                node_id=node.node_id,
                endpoint=node.endpoint,
            )
            await self.client.request_response(message, Pong)
            self.logger.info('Bonded successfully with: %s', node)

        if scope.cancelled_caught:
            self.logger.debug('Timed out bonding with: %s', node)

    async def _bootstrap(self):
        self.logger.info("Attempting to bond with %d bootnodes", len(self.bootnodes))

        async with trio.open_nursery() as nursery:
            for enode in self.bootnodes:
                nursery.start_soon(self._bond, enode)
