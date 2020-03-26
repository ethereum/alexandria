import enum
import ipaddress
import logging
from socket import (
    inet_aton,
    inet_ntoa,
)
from typing import AsyncIterable, Collection, NamedTuple, Tuple

from async_generator import asynccontextmanager
import trio

from async_service import Service
from eth_keys import keys

from .abc import ClientAPI
from .constants import DATAGRAM_BUFFER_SIZE
from .typing import NodeID


async def _bootstrap(nodes: Collection[NodeID]) -> Tuple[NodeID, ...]:
    """
    Attempt to bond with each of the provided nodes.  Return a tuple of the
    NodeID for the bonds that were successful
    """
    ...


class Events(enum.Enum):
    HANDSHAKE_SENT = 0
    HANDSHAKE_RECEIVED = 1
    HANDSHAKE_COMPLETE = 2
    HANDSHAKE_FAILED = 3


class Endpoint(NamedTuple):
    ip_address: ipaddress.IPv4Address
    port: int


class Node(NamedTuple):
    node_id: NodeID
    endpoint: Endpoint


class Datagram(NamedTuple):
    datagram: bytes
    endpoint: Endpoint


logger = logging.getLogger('alexandria.datagrams')


async def _handle_inbound(socket: trio.socket.SocketType,
                          send_channel: trio.abc.SendChannel[Datagram]) -> None:
    async with send_channel:
        while True:
            data, (ip_address, port) = await socket.recvfrom(DATAGRAM_BUFFER_SIZE)
            endpoint = Endpoint(inet_aton(ip_address), port)
            incoming_datagram = Datagram(data, endpoint)
            logger.debug('Received datagram: %s', incoming_datagram)
            await send_channel.send(incoming_datagram)


async def _handle_outbound(socket: trio.socket.SocketType,
                           receive_channel: trio.abc.SendChannel[Datagram],
                           ) -> None:
    async with receive_channel:
        async for datagram, endpoint in receive_channel:
            logger.debug('Sending datagram: %s', (datagram, endpoint))
            await socket.sendto(datagram, (inet_ntoa(endpoint.ip_address), endpoint.port))


@asynccontextmanager
async def listen(endpoint: Endpoint,
                 incoming_datagram_send_channel: trio.abc.SendChannel[Datagram],
                 outbound_datagram_receive_channel: trio.abc.ReceiveChannel[Datagram],
                 ) -> AsyncIterable[None]:
    socket = trio.socket.socket(
        family=trio.socket.AF_INET,
        type=trio.socket.SOCK_DGRAM,
    )
    ip_address, port = endpoint
    await socket.bind((str(ip_address), port))

    async with trio.open_nursery() as nursery:
        nursery.start_soon(_handle_inbound, socket, incoming_datagram_send_channel)
        nursery.start_soon(_handle_outbound, socket, outbound_datagram_receive_channel)
        yield


DEFAULT_LISTEN_ON = Endpoint('0.0.0.0', 8628)


class Client(Service, ClientAPI):
    def __init__(self,
                 private_key: keys.PrivateKey,
                 listen_on: Endpoint = DEFAULT_LISTEN_ON,
                 ) -> None:
        self.private_key = private_key
        self.public_key = private_key.public_key
        self.node_id = int(self.public_key)
        # self.routing_table = RoutingTable(self.node_id)
        self.listen_on = listen_on
        self._listening = trio.Event()

    async def wait_listening(self) -> None:
        await self._listening.wait()

    async def run(self) -> None:
        inbound_channels = trio.open_memory_channel[Datagram](0)
        outbound_channels = trio.open_memory_channel[Datagram](0)

        async with listen(self.listen_on, inbound_channels[0], outbound_channels[1]):
            self._listening.set()
            await self.manager.wait_finished()

    async def handshake(self, other: Node) -> None:
        raise NotImplementedError

    async def _handle_inbound_datagrams(self, receive_channel: trio.abc.ReceiveChannel[Datagram]):
        with receive_channel:
            async for datagram, endpoint in receive_channel:
                self.logger.debug('Received datagram from %s', endpoint)


if __name__ == "__main__":
    trio.run(...)
