import logging
from typing import Collection, Dict

from async_service import Service
from eth_keys import keys
import trio

from alexandria._utils import humanize_node_id
from alexandria.abc import ClientAPI, Endpoint, EndpointDatabaseAPI, Node, RoutingTableAPI
from alexandria.client import Client
from alexandria.endpoint_db import MemoryEndpointDB
from alexandria.kademlia import Kademlia
from alexandria.network import Network
from alexandria.routing_table import RoutingTable


BOND_TIMEOUT = 10


class Application(Service):
    logger = logging.getLogger('alexandria.app.Application')

    client: ClientAPI
    content_db: Dict[bytes, bytes]
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
        self.content_db = {}
        self.routing_table = RoutingTable(
            self.client.local_node_id,
            bucket_size=256,
        )
        self.network = Network(
            client=self.client,
            endpoint_db=self.endpoint_db,
            routing_table=self.routing_table,
        )
        self._kademlia = Kademlia(
            routing_table=self.routing_table,
            content_db=self.content_db,
            endpoint_db=self.endpoint_db,
            client=self.client,
            network=self.network,
        )

    async def run(self) -> None:
        with trio.fail_after(5):
            async with self.client.events.listening.subscribe():
                self.manager.run_daemon_child_service(self.client)

        await trio.sleep(0.1)

        self.manager.run_task(self._bootstrap)
        self.manager.run_daemon_child_service(self._kademlia)
        self.manager.run_daemon_task(self._monitor_endpoints)

        await self.manager.wait_finished()

    async def _monitor_endpoints(self) -> None:
        """
        Listen for completed handshakes and record the nodes in the routing
        table as well as the endpoint database.
        """
        async with self.client.events.handshake_complete.subscribe() as subscription:
            while self.manager.is_running:
                session = await subscription.receive()
                self.logger.debug(
                    'recording node and endpoint: %s@%s',
                    humanize_node_id(session.remote_node_id),
                    session.remote_endpoint,
                )
                self.endpoint_db.set_endpoint(session.remote_node_id, session.remote_endpoint)
                self.routing_table.update(session.remote_node_id)

    async def _bond(self, node: Node) -> None:
        with trio.move_on_after(BOND_TIMEOUT):
            await self.network.bond(node)

    async def _bootstrap(self) -> None:
        self.logger.info("Attempting to bond with %d bootnodes", len(self.bootnodes))

        async with trio.open_nursery() as nursery:
            for enode in self.bootnodes:
                nursery.start_soon(self._bond, enode)
