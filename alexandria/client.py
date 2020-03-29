import hashlib
import logging
from types import TracebackType
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterable,
    Awaitable,
    Mapping,
    Optional,
    Type,
    TypeVar,
)

from async_generator import asynccontextmanager
import trio

from async_service import Service
from eth_keys import keys
from eth_utils import humanize_hash

from alexandria.abc import ClientAPI, ConnectionAPI, Datagram, Endpoint, EventsAPI, PacketAPI
from alexandria.connection import Connection
from alexandria.constants import DATAGRAM_BUFFER_SIZE
from alexandria.packets import encode_packet, decode_packet, MessagePacket
from alexandria.session import SessionInitiator, SessionRecipient
from alexandria.typing import NodeID
from alexandria.tags import recover_source_id_from_tag, compute_tag


logger = logging.getLogger('alexandria.datagrams')


async def _handle_inbound(socket: trio.socket.SocketType,
                          send_channel: trio.abc.SendChannel[Datagram]) -> None:
    async with send_channel:
        while True:
            data, (ip_address, port) = await socket.recvfrom(DATAGRAM_BUFFER_SIZE)
            endpoint = Endpoint(ip_address, port)
            incoming_datagram = Datagram(data, endpoint)
            logger.debug('Received datagram: %s', incoming_datagram)
            try:
                await send_channel.send(incoming_datagram)
            except trio.BrokenResourceError:
                break


async def _handle_outbound(socket: trio.socket.SocketType,
                           receive_channel: trio.abc.SendChannel[Datagram],
                           ) -> None:
    async with receive_channel:
        async for datagram, endpoint in receive_channel:
            logger.debug('Sending datagram: %s', (datagram, endpoint))
            await socket.sendto(datagram, endpoint)


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
        logger.debug('Datagram listener started on %s:%s', ip_address, port)
        nursery.start_soon(_handle_inbound, socket, incoming_datagram_send_channel)
        nursery.start_soon(_handle_outbound, socket, outbound_datagram_receive_channel)
        yield


DEFAULT_LISTEN_ON = Endpoint('0.0.0.0', 8628)


TAwaitable = TypeVar('TAwaitable', bound=Awaitable[Any])


class AwaitableAsyncContextManager(Awaitable[TAwaitable],
                                   AsyncContextManager[Awaitable[TAwaitable]]):
    def __init__(self, awaitable: Awaitable[TAwaitable]) -> None:
        self._awaitable = awaitable
        self._done = False

    def __await__(self) -> TAwaitable:
        result = self._awaitable.__await__()
        self._done = True
        return result

    async def __aenter__(self) -> Awaitable[TAwaitable]:
        await trio.hazmat.checkpoint()
        return self

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        traceback: Optional[TracebackType],
                        ) -> None:
        await trio.hazmat.checkpoint()
        if exc_type is None and not self._done:
            await self


class Events(Service, EventsAPI):
    def __init__(self) -> None:
        self._new_connection_channels = trio.open_memory_channel[ConnectionAPI](0)

    async def run(self):
        self.manager.run_daemon_task(self._consume_new_connection)
        await self.manager.wait_finished()

    async def _consume_new_connection(self):
        send_channel, receive_channel = self._new_connection_channels
        async with send_channel:
            async with receive_channel:
                async for _ in receive_channel:  # noqa: F841
                    pass

    async def new_connection(self, connection: ConnectionAPI) -> None:
        await self._new_connection_channels[0].send(connection)

    def wait_new_connection(self) -> AwaitableAsyncContextManager[ConnectionAPI]:
        receive_channel = self._new_connection_channels[1].clone()

        async def get_value():
            async with receive_channel:
                return await receive_channel.receive()

        return AwaitableAsyncContextManager(get_value())


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


class Client(Service, ClientAPI):
    logger = logging.getLogger('alexandria.Client')

    _connections: Mapping[NodeID, ConnectionAPI]

    def __init__(self,
                 private_key: keys.PrivateKey,
                 listen_on: Endpoint = DEFAULT_LISTEN_ON,
                 ) -> None:
        self._private_key = private_key
        self.public_key = private_key.public_key
        self.local_node_id = int.from_bytes(sha256(self.public_key.to_bytes()), 'big')
        # self.routing_table = RoutingTable(self.local_node_id)
        self.listen_on = listen_on
        self._listening = trio.Event()
        self._connections = {}
        self._connections_lock = trio.Lock()
        self.events = Events()

    async def wait_listening(self) -> None:
        await self._listening.wait()

    async def run(self) -> None:
        inbound_channels = trio.open_memory_channel[Datagram](0)
        outbound_channels = trio.open_memory_channel[Datagram](0)

        self._outbound_datagram_send_channel = outbound_channels[0]

        async with listen(self.listen_on, inbound_channels[0], outbound_channels[1]):
            self.manager.run_daemon_child_service(self.events)
            self.manager.run_daemon_task(self._handle_inbound_datagrams, inbound_channels[1])
            self._listening.set()
            self.logger.info(
                'Client running: %s@%s:%s',
                humanize_hash(self.local_node_id.to_bytes(32, 'big')),
                self.listen_on[0],
                self.listen_on[1],
            )
            await self.manager.wait_finished()

    async def ping(self, remote_node_id: NodeID, remote_endpoint: Endpoint) -> None:
        # message = Ping()
        packet = MessagePacket(tag=compute_tag(self.local_node_id, remote_node_id))
        connection = await self._get_connection(remote_node_id, remote_endpoint, is_initiator=False)
        await connection.send_packet(packet)

    async def _manage_connection(self, connection: ConnectionAPI):
        """
        - run the connection as a child service.
        """
        self.manager.run_child_service(connection)

        remote_node_id = connection.session.remote_node_id
        self._connections[remote_node_id] = connection
        # signal that a new connection was made.
        await self.events.new_connection(connection)

        outbound_packet_receive_channel = connection.outbound_packet_receive_channel

        try:
            async with outbound_packet_receive_channel:
                async for packet in outbound_packet_receive_channel:
                    await self._outbound_datagram_send_channel.send(Datagram(
                        encode_packet(packet),
                        connection.remote_endpoint,
                    ))
        finally:
            async with self._connections_lock:
                self._connections.pop(remote_node_id)

    async def _get_connection(self,
                              remote_node_id: NodeID,
                              remote_endpoint: Endpoint,
                              *,
                              is_initiator: bool) -> Connection:
        async with self._connections_lock:
            if remote_node_id in self._connections:
                return self._connections[remote_node_id]
            else:
                if is_initiator:
                    session = SessionInitiator(
                        is_initiator=True,
                        private_key=self._private_key,
                        remote_node_id=remote_node_id,
                    )
                else:
                    session = SessionRecipient(
                        is_initiator=True,
                        private_key=self._private_key,
                        remote_node_id=remote_node_id,
                    )
                inbound_channels = trio.open_memory_channel[PacketAPI](0)
                outbound_channels = trio.open_memory_channel[PacketAPI](0)
                connection = Connection(
                    session=session,
                    remote_endpoint=remote_endpoint,
                    inbound_packet_send_channel=inbound_channels[0],
                    inbound_packet_receive_channel=inbound_channels[1],
                    outbound_packet_send_channel=outbound_channels[0],
                    outbound_packet_receive_channel=outbound_channels[1],
                )
                self.manager.run_task(self._manage_connection, connection)
                await connection.wait_ready()
                return connection

    async def _handle_inbound_datagrams(self, receive_channel: trio.abc.ReceiveChannel[Datagram]):
        async with receive_channel:
            async with trio.open_nursery() as nursery:
                async for datagram, endpoint in receive_channel:
                    packet = decode_packet(datagram)
                    remote_node_id = recover_source_id_from_tag(packet.tag, self.local_node_id)
                    connection = await self._get_connection(
                        remote_node_id,
                        endpoint,
                        is_initiator=False,
                    )
                    nursery.start_soon(connection.inbound_packet_send_channel.send, packet)
                    self.logger.debug('Received datagram from %s', endpoint)
