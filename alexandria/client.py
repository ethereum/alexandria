import hashlib
import logging
from typing import (
    Mapping,
    Type,
)

from ssz import sedes
import trio

from async_service import Service
from eth_keys import keys
from eth_utils import humanize_hash

from alexandria.abc import (
    ClientAPI,
    Datagram,
    Endpoint,
    MessageAPI,
    PacketAPI,
    TPayload,
    SessionAPI,
    SubscriptionAPI,
)
from alexandria.datagrams import listen
from alexandria.events import Events
from alexandria.messages import Message, Ping, Pong
from alexandria.packets import encode_packet, decode_packet
from alexandria.session import SessionInitiator, SessionRecipient
from alexandria.subscriptions import SubscriptionManager
from alexandria.typing import NodeID
from alexandria.tags import recover_source_id_from_tag


DEFAULT_LISTEN_ON = Endpoint('0.0.0.0', 8628)


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


class Client(Service, ClientAPI):
    logger = logging.getLogger('alexandria.client.Client')

    _sessions: Mapping[NodeID, SessionAPI]

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
        self._sessions = {}
        self.events = Events()

    async def wait_listening(self) -> None:
        await self._listening.wait()

    async def run(self) -> None:
        inbound_channels = trio.open_memory_channel[Datagram](0)
        outbound_channels = trio.open_memory_channel[Datagram](0)

        inbound_message_channels = trio.open_memory_channel[MessageAPI[sedes.Serializable]](256)
        self._inbound_message_send_channel = inbound_message_channels[0]
        self._subscription_manager = SubscriptionManager(inbound_message_channels[1])

        self._outbound_datagram_send_channel = outbound_channels[0]

        async with listen(self.listen_on, inbound_channels[0], outbound_channels[1]):
            self.manager.run_daemon_task(self._handle_inbound_datagrams, inbound_channels[1])
            # Run the subscription manager with gets fed all decoded inbound
            # messages and dispatches them to individual subscriptions.
            self.manager.run_daemon_child_service(self._subscription_manager)
            self._listening.set()
            self.logger.info(
                'Client running: %s@%s:%s',
                humanize_hash(self.local_node_id.to_bytes(32, 'big')),
                self.listen_on[0],
                self.listen_on[1],
            )
            await self.manager.wait_finished()

    def subscribe(self, payload_type: Type[TPayload]) -> SubscriptionAPI[MessageAPI[TPayload]]:
        return self._subscription_manager.subscribe(payload_type)

    async def ping(self, ping_id: int, remote_node_id: NodeID, remote_endpoint: Endpoint) -> None:
        payload = Ping(id=ping_id)
        message = Message(
            payload=payload,
            node_id=remote_node_id,
            endpoint=remote_endpoint,
        )
        session = self._get_session(remote_node_id, remote_endpoint, is_initiator=True)
        await session.handle_outbound_message(message)

    async def pong(self, ping_id: int, remote_node_id: NodeID, remote_endpoint: Endpoint) -> None:
        payload = Pong(ping_id=ping_id)
        message = Message(
            payload=payload,
            node_id=remote_node_id,
            endpoint=remote_endpoint,
        )
        session = self._get_session(remote_node_id, remote_endpoint, is_initiator=True)
        await session.handle_outbound_message(message)

    async def _manage_session(self,
                              session: SessionAPI,
                              receive_channel: trio.abc.ReceiveChannel[PacketAPI]):
        if session.remote_node_id not in self._sessions:
            raise Exception("Session not found in managed session")

        # signal that a new session was made.
        await self.events.new_session(session)

        try:
            async with receive_channel:
                async for packet in receive_channel:
                    await self._outbound_datagram_send_channel.send(Datagram(
                        encode_packet(packet),
                        session.remote_endpoint,
                    ))
                    self.logger.debug('Dispatching outbound packet from %s', session)
        finally:
            self._sessions.pop(session.remote_node_id)

    def _get_session(self,
                     remote_node_id: NodeID,
                     remote_endpoint: Endpoint,
                     *,
                     is_initiator: bool) -> SessionAPI:
        if remote_node_id in self._sessions:
            return self._sessions[remote_node_id]
        else:
            outbound_channels = trio.open_memory_channel[PacketAPI](0)

            if is_initiator:
                session = SessionInitiator(
                    private_key=self._private_key,
                    remote_node_id=remote_node_id,
                    remote_endpoint=remote_endpoint,
                    outbound_packet_send_channel=outbound_channels[0],
                    inbound_message_send_channel=self._inbound_message_send_channel.clone(),
                )
            else:
                session = SessionRecipient(
                    private_key=self._private_key,
                    remote_node_id=remote_node_id,
                    remote_endpoint=remote_endpoint,
                    outbound_packet_send_channel=outbound_channels[0],
                    inbound_message_send_channel=self._inbound_message_send_channel.clone(),
                )

            # We insert the session here to prevent a race condition where an
            # alternate path to session creation results in a new session being
            # inserted before the `_manage_session` task can run.
            self._sessions[remote_node_id] = session

            self.manager.run_task(self._manage_session, session, outbound_channels[1])
            return session

    async def _handle_inbound_datagrams(self, receive_channel: trio.abc.ReceiveChannel[Datagram]):
        async with receive_channel:
            async with trio.open_nursery() as nursery:
                async for datagram, endpoint in receive_channel:
                    packet = decode_packet(datagram)
                    remote_node_id = recover_source_id_from_tag(packet.tag, self.local_node_id)
                    session = self._get_session(
                        remote_node_id,
                        endpoint,
                        is_initiator=False,
                    )
                    nursery.start_soon(session.handle_inbound_packet, packet)
                    self.logger.debug('Dispatching inbound packet to %s', session)
