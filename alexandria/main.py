import argparse
import ipaddress
import logging
import os
import pathlib
import secrets
from typing import Collection, Optional

from async_service import background_trio_service, Service
from eth_keys import keys
from eth_utils import encode_hex, decode_hex
import trio

from alexandria._utils import sha256, every
from alexandria.abc import Endpoint, Node, DurableDatabaseAPI
from alexandria.app import Application
from alexandria.cli_parser import parser
from alexandria.config import DEFAULT_CONFIG, KademliaConfig
from alexandria.durable_db import DurableDB
from alexandria.logging import setup_logging
from alexandria.metrics import Metrics
from alexandria.rpc import RPCServer
from alexandria.upnp import setup_port_map, UPNP_PORTMAP_DURATION, PortMapFailed
from alexandria.xdg import get_xdg_alexandria_root

ALEXANDRIA_HEADER = "\n".join((
    "",
    r"           _                          _      _       ",
    r"     /\   | |                        | |    (_)      ",
    r"    /  \  | | _____  ____ _ _ __   __| |_ __ _  __ _ ",
    r"   / /\ \ | |/ _ \ \/ / _` | '_ \ / _` | '__| |/ _` |",
    r"  / ____ \| |  __/>  < (_| | | | | (_| | |  | | (_| |",
    r" /_/    \_\_|\___/_/\_\__,_|_| |_|\__,_|_|  |_|\__,_|",
    "",
))


class Alexandria(Service):
    logger = logging.getLogger('alexandria')
    metrics: Optional[Metrics]

    def __init__(self,
                 private_key: keys.PrivateKey,
                 listen_on: Endpoint,
                 bootnodes: Collection[Node],
                 durable_db: DurableDatabaseAPI,
                 kademlia_config: KademliaConfig,
                 ipc_path: pathlib.Path,
                 metrics_args: argparse.Namespace = None,
                 ) -> None:
        self.application = Application(
            bootnodes=bootnodes,
            private_key=private_key,
            listen_on=listen_on,
            durable_db=durable_db,
            config=kademlia_config,
        )
        self.json_rpc_server = RPCServer(
            ipc_path=ipc_path,
            client=self.application.client,
            network=self.application.network,
            kademlia=self.application.kademlia,
            routing_table=self.application.routing_table,
        )
        if metrics_args is not None:
            self.metrics = Metrics.from_cli_args(
                metrics_args,
                client=self.application.client,
                kademlia=self.application.kademlia,
            )
        else:
            self.metrics = None

    async def run(self) -> None:
        self.logger.info("Node: %s", self.application.client.local_node.node_uri)
        self.manager.run_daemon_task(self._manage_upnp)
        self.manager.run_daemon_child_service(self.application)
        self.manager.run_daemon_child_service(self.json_rpc_server)
        if self.metrics is not None:
            self.manager.run_daemon_child_service(self.metrics)
        await self.manager.wait_finished()

    async def _manage_upnp(self) -> None:
        while self.manager.is_running:
            async for _ in every(UPNP_PORTMAP_DURATION):
                try:
                    internal_ip, external_ip = await trio.to_thread.run_sync(
                        setup_port_map,
                        self.application.client.listen_on.port,
                        UPNP_PORTMAP_DURATION,
                    )
                except PortMapFailed:
                    continue
                external_endpoint = Endpoint(
                    ipaddress.IPv4Address(external_ip),
                    self.application.client.listen_on.port,
                )
                await self.application.client.events.new_external_ip.trigger(external_endpoint)


DEFAULT_BOOTNODES = (
    Node.from_node_uri('node://157d841a79faa0dc11180724ecca44322fa07f9b5b8950e4f10c13dcbac9e074@74.207.253.18:30314'),  # noqa: E501
    Node.from_node_uri('node://b8fe2b71b9138e65ee48e6a0ab3ebd63622c8e2e46c963cbf71bc351132b39af@192.155.84.246:30314'),  # noqa: E501
)


async def main() -> None:
    DEFAULT_LISTEN_ON = Endpoint(ipaddress.IPv4Address('0.0.0.0'), 30314)

    args = parser.parse_args()
    setup_logging(args.log_level)

    if args.port is not None:
        listen_on = Endpoint(ipaddress.IPv4Address('0.0.0.0'), args.port)
    else:
        listen_on = DEFAULT_LISTEN_ON

    logger = logging.getLogger()

    if args.bootnodes is not None:
        bootnodes = tuple(
            Node.from_node_uri(node_uri) for node_uri in args.bootnodes
        )
    else:
        bootnodes = DEFAULT_BOOTNODES

    application_root_dir = get_xdg_alexandria_root()
    if not application_root_dir.exists():
        application_root_dir.mkdir(parents=True, exist_ok=True)

    ipc_path = application_root_dir / 'jsonrpc.ipc'

    if args.private_key_seed is None:
        node_key_path = application_root_dir / 'nodekey'
        if node_key_path.exists():
            private_key_hex = node_key_path.read_text().strip()
            private_key_bytes = decode_hex(private_key_hex)
            private_key = keys.PrivateKey(private_key_bytes)
        else:
            private_key_bytes = secrets.token_bytes(32)
            node_key_path.write_text(encode_hex(private_key_bytes))
            private_key = keys.PrivateKey(private_key_bytes)
    else:
        private_key = keys.PrivateKey(sha256(args.private_key_seed))

    durable_db_path = application_root_dir / 'durable-db'
    durable_db = DurableDB(durable_db_path)

    metrics_args: Optional[argparse.Namespace]
    if args.enable_metrics:
        metrics_args = args
    else:
        metrics_args = None

    alexandria = Alexandria(
        private_key=private_key,
        listen_on=listen_on,
        bootnodes=bootnodes,
        durable_db=durable_db,
        kademlia_config=DEFAULT_CONFIG,
        ipc_path=ipc_path,
        metrics_args=metrics_args,
    )

    logger.info(ALEXANDRIA_HEADER)
    logger.info("Started main process (pid=%d)", os.getpid())
    async with background_trio_service(alexandria) as manager:
        await manager.wait_finished()
