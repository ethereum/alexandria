import logging
from typing import Collection

from async_service import Service
from eth_keys import keys
import trio

from alexandria._utils import humanize_node_id
from alexandria.abc import (
    ClientAPI,
    DurableDatabaseAPI,
    Endpoint,
    EndpointDatabaseAPI,
    KademliaAPI,
    Node,
    RoutingTableAPI,
)
from alexandria.client import Client
from alexandria.endpoint_db import MemoryEndpointDB
from alexandria.kademlia import Kademlia, KademliaConfig
from alexandria.network import Network
from alexandria.routing_table import RoutingTable


BOND_TIMEOUT = 5


class Application(Service):
    logger = logging.getLogger('alexandria.app.Application')

    client: ClientAPI
    durable_db: DurableDatabaseAPI
    endpoint_db: EndpointDatabaseAPI
    routing_table: RoutingTableAPI
    config: KademliaConfig
    kademlia: KademliaAPI

    def __init__(self,
                 bootnodes: Collection[Node],
                 private_key: keys.PrivateKey,
                 listen_on: Endpoint,
                 durable_db: DurableDatabaseAPI,
                 config: KademliaConfig,
                 ) -> None:
        self.config = config
        self.client = Client(
            private_key=private_key,
            listen_on=listen_on,
        )
        self.bootnodes = tuple(
            node for node in bootnodes
            if node.node_id != self.client.local_node_id
        )
        self._bonded = trio.Event()
        self.endpoint_db = MemoryEndpointDB()
        self.durable_db = durable_db
        self.routing_table = RoutingTable(
            self.client.local_node_id,
            bucket_size=256,
        )
        self.network = Network(
            client=self.client,
            endpoint_db=self.endpoint_db,
            routing_table=self.routing_table,
        )
        self.kademlia = Kademlia(
            routing_table=self.routing_table,
            durable_db=self.durable_db,
            endpoint_db=self.endpoint_db,
            client=self.client,
            network=self.network,
            config=self.config,
        )

    async def run(self) -> None:
        self.manager.run_daemon_child_service(self.client)
        self.manager.run_daemon_task(self._monitor_endpoints)
        self.manager.run_daemon_child_service(self.kademlia)

        with trio.fail_after(5):
            await self.client.wait_ready()

        self.manager.run_task(self._bootstrap)
        await self._bonded.wait()
        self.logger.info("Bonding Successful!!!")

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
        self.logger.debug('Attempting bond with %s', node)
        with trio.move_on_after(BOND_TIMEOUT):
            await self.network.bond(node)
            self._bonded.set()

    async def _bootstrap(self) -> None:
        async with trio.open_nursery() as nursery:
            while self.manager.is_running:
                self.logger.info("Attempting to bond with %d bootnodes", len(self.bootnodes))

                if not self.routing_table.is_empty:
                    self._bonded.set()
                    break

                for node in self.bootnodes:
                    nursery.start_soon(self._bond, node)

                with trio.move_on_after(BOND_TIMEOUT * 2) as scope:
                    await self._bonded.wait()
                    break

                if scope.cancelled_caught:
                    self.logger.info("Bonding failed... trying again")
