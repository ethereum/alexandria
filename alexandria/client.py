import logging
from typing import Collection, Tuple, Type

from async_service import Service, background_trio_service
from eth_keys import keys
from eth_utils import ValidationError
from eth_utils.toolz import partition_all
from ssz import sedes
import trio

from alexandria._utils import (
    public_key_to_node_id,
    humanize_node_id,
    split_data_to_chunks,
)
from alexandria.abc import (
    ClientAPI,
    Datagram,
    Endpoint,
    MessageAPI,
    Node,
    SessionAPI,
    TPayload,
)
from alexandria.constants import NODES_PER_PAYLOAD
from alexandria.datagrams import DatagramListener
from alexandria.exceptions import SessionNotFound
from alexandria.events import Events
from alexandria.packets import decode_packet, NetworkPacket
from alexandria.pool import Pool
from alexandria.messages import Message
from alexandria.message_dispatcher import MessageDispatcher
from alexandria.payloads import (
    FindNodes, FoundNodes,
    Ping, Pong,
    Advertise, Ack,
    Locate, Locations,
    Retrieve, Chunk,
)
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
        self.local_node = Node(self.local_node_id, self.listen_on)

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
            events=self.events,
            outbound_packet_send_channel=self._outbound_packet_send_channel,
            inbound_message_send_channel=self._inbound_message_send_channel,
        )

        self.message_dispatcher = MessageDispatcher(
            self._outbound_message_send_channel,
            self._inbound_message_receive_channel,
        )

    #
    # Send Mesasges: Routing
    #
    async def send_ping(self, node: Node) -> int:
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Ping(request_id),
            node,
        )
        self.logger.info("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        return request_id

    async def send_pong(self, node: Node, *, request_id: int) -> None:
        message = Message(
            Pong(request_id),
            node,
        )
        self.logger.info("Sending %s", message)
        await self.message_dispatcher.send_message(message)

    async def send_find_nodes(self, node: Node, *, distance: int) -> int:
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            FindNodes(request_id, distance),
            node,
        )
        self.logger.info("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        return request_id

    async def send_found_nodes(self,
                               node: Node,
                               *,
                               request_id: int,
                               found_nodes: Collection[Node]) -> int:
        batches = tuple(partition_all(NODES_PER_PAYLOAD, found_nodes))
        self.logger.info("Sending FoundNodes with %d nodes to %s", len(found_nodes), node)
        if batches:
            total_batches = len(batches)
            for batch in batches:
                payload = tuple(
                    (node.node_id, node.endpoint.ip_address.packed, node.endpoint.port)
                    for node in batch
                )
                response = Message(
                    FoundNodes(request_id, total_batches, payload),
                    node,
                )
                await self.message_dispatcher.send_message(response)
            return total_batches
        else:
            response = Message(
                FoundNodes(request_id, 1, ()),
                node,
            )
            await self.message_dispatcher.send_message(response)
            return 1

    #
    # Send Messages: Content
    #
    async def send_advertise(self, node: Node, *, key: bytes, who: Node) -> int:
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Advertise(request_id, key),
            node,
        )
        self.logger.info("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        return request_id

    async def send_ack(self, node: Node, *, request_id: int) -> None:
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Ack(request_id),
            node,
        )
        self.logger.info("Sending %s", message)
        await self.message_dispatcher.send_message(message)

    async def send_locate(self, node: Node, *, key: bytes) -> int:
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Locate(request_id, key),
            node,
        )
        self.logger.info("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        return request_id

    async def send_locations(self,
                             node: Node,
                             *,
                             request_id: int,
                             locations: Collection[Node]) -> int:
        batches = tuple(partition_all(NODES_PER_PAYLOAD, locations))
        self.logger.info("Sending Locations with %d nodes to %s", len(locations), node)
        if batches:
            total_batches = len(batches)
            for batch in batches:
                payload = tuple(
                    (node.node_id, node.endpoint.ip_address.packed, node.endpoint.port)
                    for node in batch
                )
                response = Message(
                    Locations(request_id, total_batches, payload),
                    node,
                )
                await self.message_dispatcher.send_message(response)
            return total_batches
        else:
            response = Message(
                Locations(request_id, 1, ()),
                node,
            )
            await self.message_dispatcher.send_message(response)
            return 1

    async def send_retrieve(self,
                            node: Node,
                            *,
                            key: bytes) -> int:
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Retrieve(request_id, key),
            node,
        )
        self.logger.info("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        return request_id

    async def send_chunks(self,
                          node: Node,
                          *,
                          request_id: int,
                          data: bytes) -> int:
        if not data:
            response = Message(
                Chunk(request_id, 1, 0, b''),
                node,
            )
            await self.message_dispatcher.send_message(response)
            return 1

        all_chunks = split_data_to_chunks(data)
        total_chunks = len(all_chunks)

        for index, chunk in enumerate(all_chunks):
            response = Message(
                Chunk(request_id, total_chunks, index, chunk),
                node,
            )
            await self.message_dispatcher.send_message(response)

        return total_chunks

    #
    # Request/Response
    #
    async def ping(self, node: Node) -> MessageAPI[Pong]:
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(Ping(request_id), node)
        with self.message_dispatcher.subscribe_request(message, Pong) as subscription:
            return await subscription.receive()

    async def find_nodes(self, node: Node, *, distance: int) -> Tuple[MessageAPI[FoundNodes], ...]:
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(FindNodes(request_id, distance), node)
        return await self._do_request_with_multi_response(message, FoundNodes)

    async def advertise(self, node: Node, *, key: bytes, who: Node) -> MessageAPI[Ack]:
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        node_payload = (who.node_id, who.endpoint.ip_address.packed, who.endpoint.port)
        message = Message(Advertise(request_id, key, node_payload), who)
        with self.message_dispatcher.subscribe_request(message, Ack) as subscription:
            return await subscription.receive()

    async def locate(self, node: Node, *, key: bytes) -> Tuple[MessageAPI[Locations], ...]:
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(Locate(request_id, key), node)
        await self._do_request_with_multi_response(message, Locations)

    async def retrieve(self, node: Node, *, key: bytes) -> Tuple[MessageAPI[Chunk], ...]:
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(Retrieve(request_id, key), node)
        await self._do_request_with_multi_response(message, Chunk)

    async def _do_request_with_multi_response(self,
                                              request: MessageAPI[sedes.Serializable],
                                              response_payload_type: Type[TPayload],
                                              ) -> Tuple[MessageAPI[TPayload], ...]:
        subscription = self.message_dispatcher.subscribe_request(request, response_payload_type)
        with subscription:
            responses = []
            total_messages = None
            while True:
                response = await subscription.receive()
                if total_messages is None:
                    total_messages = response.payload.total
                else:
                    if response.payload.total != total_messages:
                        raise ValidationError(
                            f"Inconsistent total message count.  First message "
                            f"indicated {total_messages} total message.  Message "
                            f"#{len(responses)} indicated "
                            f"{response.payload.total}."
                        )
                responses.append(response)
                if len(responses) >= total_messages:
                    break
            return tuple(responses)

    #
    # Utilities
    #
    async def _get_or_create_session(self,
                                     node: Node,
                                     *,
                                     is_initiator: bool) -> SessionAPI:
        try:
            session = self.pool.get_session(node.node_id)
        except SessionNotFound:
            session = self.pool.create_session(
                node,
                is_initiator=is_initiator,
            )
            await self.events.new_session.trigger(session)

        return session

    #
    # Service APIs
    #
    async def run(self) -> None:
        # Run the subscription manager with gets fed all decoded inbound
        # messages and dispatches them to individual subscriptions.
        self.manager.run_daemon_child_service(self.message_dispatcher)

        listener = DatagramListener(
            self.listen_on,
            self._inbound_datagram_send_channel,
            self._outbound_datagram_receive_channel,
        )
        async with background_trio_service(listener):
            await listener.wait_listening()
            await self.events.listening.trigger(self.listen_on)

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
            self.logger.info(
                'Client running: %s@%s',
                humanize_node_id(self.local_node_id),
                self.listen_on,
            )
            await self.manager.wait_finished()

    async def _handle_outbound_messages(self,
                                        receive_channel: trio.abc.ReceiveChannel[MessageAPI],
                                        ) -> None:
        async with trio.open_nursery() as nursery:
            async with receive_channel:
                async for message in receive_channel:
                    session = await self._get_or_create_session(
                        message.node,
                        is_initiator=True,
                    )
                    nursery.start_soon(session.handle_outbound_message, message)
                    self.logger.debug('outbound message %s dispatched to %s', message, session)

    async def _handle_outbound_packets(self,
                                       receive_channel: trio.abc.ReceiveChannel[NetworkPacket],
                                       ) -> None:
        async with receive_channel:
            async for packet in receive_channel:
                await self._outbound_datagram_send_channel.send(packet.as_datagram())
                self.logger.debug('packet > %s', packet)

    async def _handle_inbound_datagrams(self, receive_channel: trio.abc.ReceiveChannel[Datagram]):
        async with trio.open_nursery() as nursery:
            async with receive_channel:
                async for datagram in receive_channel:
                    packet = decode_packet(datagram.data)
                    remote_node_id = recover_source_id_from_tag(packet.tag, self.local_node_id)
                    remote_node = Node(remote_node_id, datagram.endpoint)
                    session = await self._get_or_create_session(
                        remote_node,
                        is_initiator=False,
                    )

                    nursery.start_soon(session.handle_inbound_packet, packet)
                    self.logger.debug('decoded datagram %s dispatched to %s', datagram, session)
