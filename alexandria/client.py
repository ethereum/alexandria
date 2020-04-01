import logging

import trio

from async_service import Service
from eth_keys import keys

from alexandria._utils import public_key_to_node_id, humanize_node_id
from alexandria.abc import (
    ClientAPI,
    Datagram,
    Endpoint,
    MessageAPI,
    SessionAPI,
)
from alexandria.datagrams import listen
from alexandria.exceptions import SessionNotFound
from alexandria.events import Events
from alexandria.packets import decode_packet, NetworkPacket
from alexandria.pool import Pool
from alexandria.message_dispatcher import MessageDispatcher
from alexandria.typing import NodeID
from alexandria.tags import recover_source_id_from_tag


class Client(Service, ClientAPI):
    logger = logging.getLogger('alexandria.client.Client')

    def __init__(self,
                 private_key: keys.PrivateKey,
                 listen_on: Endpoint,
                 ) -> None:
        self._private_key = private_key
        self.public_key = private_key.public_key
        self.local_node_id = public_key_to_node_id(self.public_key)

        self.listen_on = listen_on
        self._listening = trio.Event()

        # Datagrams
        (
            self._outbound_datagram_send_channel,
            self._outbound_datagram_receive_channel,
        ) = trio.open_memory_channel[Datagram](0)
        (
            self._inbound_datagram_send_channel,
            self._inbound_datagram_receive_channel,
        ) = trio.open_memory_channel[Datagram](0)

        # Packets
        (
            self._outbound_packet_send_channel,
            self._outbound_packet_receive_channel,
        ) = trio.open_memory_channel[NetworkPacket](0)
        (
            self._inbound_packet_send_channel,
            self._inbound_packet_receive_channel,
        ) = trio.open_memory_channel[NetworkPacket](0)

        # Messages
        (
            self._outbound_message_send_channel,
            self._outbound_message_receive_channel,
        ) = trio.open_memory_channel[MessageAPI](0)
        (
            self._inbound_message_send_channel,
            self._inbound_message_receive_channel,
        ) = trio.open_memory_channel[MessageAPI](0)

        self.events = Events()
        self.pool = Pool(
            private_key=self._private_key,
            outbound_packet_send_channel=self._outbound_packet_send_channel,
            inbound_message_send_channel=self._inbound_message_send_channel,
        )

        self.message_dispatcher = MessageDispatcher(
            self._outbound_message_send_channel,
            self._inbound_message_receive_channel,
        )

    async def wait_listening(self) -> None:
        await self._listening.wait()

    async def run(self) -> None:
        # Run the subscription manager with gets fed all decoded inbound
        # messages and dispatches them to individual subscriptions.
        self.manager.run_daemon_child_service(self.message_dispatcher)

        listener = listen(
            self.listen_on,
            self._inbound_datagram_send_channel,
            self._outbound_datagram_receive_channel,
        )
        async with listener:
            self.manager.run_daemon_task(
                self._handle_inbound_datagrams,
                self._inbound_datagram_receive_channel,
            )
            self.manager.run_daemon_task(
                self._handle_outbound_messages,
                self._outbound_message_receive_channel,
            )
            self.manager.run_daemon_task(
                self._handle_outbound_packets,
                self._outbound_packet_receive_channel,
            )
            self._listening.set()
            self.logger.info(
                'Client running: %s@%s',
                humanize_node_id(self.local_node_id),
                self.listen_on,
            )
            await self.manager.wait_finished()

    async def _get_or_create_session(self,
                                     node_id: NodeID,
                                     endpoint: Endpoint,
                                     *,
                                     is_initiator: bool) -> SessionAPI:
        try:
            session = self.pool.get_session(node_id)
        except SessionNotFound:
            session = self.pool.create_session(
                node_id,
                endpoint,
                is_initiator=is_initiator,
            )
            await self.events.new_session(session)

        return session

    async def _handle_outbound_messages(self,
                                        receive_channel: trio.abc.ReceiveChannel[MessageAPI],
                                        ) -> None:
        async with trio.open_nursery() as nursery:
            async with receive_channel:
                async for message in receive_channel:
                    session = await self._get_or_create_session(
                        message.node_id,
                        message.endpoint,
                        is_initiator=True,
                    )
                    nursery.start_soon(session.handle_outbound_message, message)
                    self.logger.debug('Dispatching outbound message: %s', message)

    async def _handle_outbound_packets(self,
                                       receive_channel: trio.abc.ReceiveChannel[NetworkPacket],
                                       ) -> None:
        async with receive_channel:
            async for packet in receive_channel:
                await self._outbound_datagram_send_channel.send(packet.as_datagram())
                self.logger.debug('Dispatching outbound packet: %s', packet)

    async def _handle_inbound_datagrams(self, receive_channel: trio.abc.ReceiveChannel[Datagram]):
        async with trio.open_nursery() as nursery:
            async with receive_channel:
                async for datagram in receive_channel:
                    packet = decode_packet(datagram.data)
                    remote_node_id = recover_source_id_from_tag(packet.tag, self.local_node_id)
                    session = await self._get_or_create_session(
                        remote_node_id,
                        datagram.endpoint,
                        is_initiator=False,
                    )

                    nursery.start_soon(session.handle_inbound_packet, packet)
                    self.logger.debug('Dispatching inbound packet to %s', session)
