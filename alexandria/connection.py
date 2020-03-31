import logging
from typing import (
    DefaultDict,
    Set,
    Type,
)

from async_service import Service
from eth_utils import humanize_hash
import trio

from alexandria.abc import (
    ConnectionAPI,
    Endpoint,
    MessageAPI,
    PacketAPI,
    SessionAPI,
)


class Connection(Service, ConnectionAPI):
    logger = logging.getLogger('alexandria.Connection')

    _subscriptions: DefaultDict[Type[MessageAPI], Set[trio.abc.SendChannel[MessageAPI]]]

    def __init__(self,
                 session: SessionAPI,
                 remote_endpoint: Endpoint,
                 ) -> None:
        self.session = session
        self.remote_endpoint = remote_endpoint
        self._ready = trio.Event()

    async def wait_ready(self) -> None:
        await self._ready.wait()

    async def run(self) -> None:
        self.logger.info(
            'Running connection: %s@%s',
            humanize_hash(self.session.remote_node_id.to_bytes(32, 'big')),
            self.remote_endpoint,
        )
        self._ready.set()
        await self.manager.wait_finished()

    async def send_packet(self, packet: PacketAPI) -> None:
        await self.session.handle_outbound_packet(packet)

    async def send_message(self, message: MessageAPI) -> None:
        await self.session.handle_outbound_message(message)
