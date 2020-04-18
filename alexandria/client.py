import logging
from typing import Collection, Optional, Tuple, Type

from async_service import Service, background_trio_service
from eth_keys import keys
from eth_utils import ValidationError
from eth_utils.toolz import partition_all
from ssz import sedes
import trio

from alexandria._utils import (
    graph_key_to_content_key,
    public_key_to_node_id,
    humanize_node_id,
    split_data_to_chunks,
    every,
)
from alexandria.abc import (
    ClientAPI,
    Datagram,
    Endpoint,
    MessageAPI,
    NetworkPacket,
    Node,
    SessionAPI,
    SGNodeAPI,
    TPayload,
)
from alexandria.constants import (
    CHUNK_MAX_SIZE,
    NODES_PER_PAYLOAD,
    HANDSHAKE_TIMEOUT,
    SESSION_IDLE_TIMEOUT,
    PING_TIMEOUT,
)
from alexandria.datagrams import DatagramListener
from alexandria.exceptions import SessionNotFound, DecryptionError, CorruptSession
from alexandria.events import Events
from alexandria.packets import decode_packet
from alexandria.pool import Pool
from alexandria.messages import Message
from alexandria.message_dispatcher import MessageDispatcher
from alexandria.payloads import (
    FindNodes, FoundNodes,
    Ping, Pong,
    Advertise, Ack,
    Locate, Locations,
    Retrieve, Chunk,
    SkipGraphNode,
    GraphGetIntroduction, GraphIntroduction,
    GraphGetNode, GraphNode,
    GraphInsert, GraphInserted,
    GraphDelete, GraphDeleted,
)
from alexandria.tags import recover_source_id_from_tag
from alexandria.typing import Key


class Client(Service, ClientAPI):
    logger = logging.getLogger('alexandria.client.Client')
    _external_endpoint: Optional[Endpoint] = None

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
        ) = trio.open_memory_channel[MessageAPI[sedes.Serializable]](0)
        (
            self._inbound_message_send_channel,
            self._inbound_message_receive_channel,
        ) = trio.open_memory_channel[MessageAPI[sedes.Serializable]](0)

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

        self._ready = trio.Event()

    @property
    def external_endpoint(self) -> Endpoint:
        if self._external_endpoint is None:
            return self.listen_on
        else:
            return self._external_endpoint

    async def wait_ready(self) -> None:
        await self._ready.wait()

    #
    # Send Mesasges: Routing
    #
    async def send_ping(self, node: Node) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Ping(request_id),
            node,
        )
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_ping.trigger(message)
        return request_id

    async def send_pong(self, node: Node, *, request_id: int) -> None:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        message = Message(
            Pong(request_id),
            node,
        )
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_pong.trigger(message)

    async def send_find_nodes(self, node: Node, *, distance: int) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            FindNodes(request_id, distance),
            node,
        )
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_find_nodes.trigger(message)
        return request_id

    async def send_found_nodes(self,
                               node: Node,
                               *,
                               request_id: int,
                               found_nodes: Collection[Node]) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        batches = tuple(partition_all(NODES_PER_PAYLOAD, found_nodes))
        self.logger.debug("Sending FoundNodes with %d nodes to %s", len(found_nodes), node)
        if batches:
            total_batches = len(batches)
            for batch in batches:
                payload = tuple(
                    node.to_payload()
                    for node in batch
                )
                response = Message(
                    FoundNodes(request_id, total_batches, payload),
                    node,
                )
                await self.message_dispatcher.send_message(response)
                await self.events.sent_found_nodes.trigger(response)
            return total_batches
        else:
            response = Message(
                FoundNodes(request_id, 1, ()),
                node,
            )
            await self.message_dispatcher.send_message(response)
            await self.events.sent_found_nodes.trigger(response)
            return 1

    #
    # Send Messages: Content
    #
    async def send_advertise(self, node: Node, *, key: bytes, who: Node) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Advertise(request_id, key, who.to_payload()),
            node,
        )
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_advertise.trigger(message)
        return request_id

    async def send_ack(self, node: Node, *, request_id: int) -> None:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        message = Message(
            Ack(request_id),
            node,
        )
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_ack.trigger(message)

    async def send_locate(self, node: Node, *, key: bytes) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Locate(request_id, key),
            node,
        )
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_locate.trigger(message)
        return request_id

    async def send_locations(self,
                             node: Node,
                             *,
                             request_id: int,
                             locations: Collection[Node]) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        batches = tuple(partition_all(NODES_PER_PAYLOAD, locations))
        self.logger.debug("Sending Locations with %d nodes to %s", len(locations), node)
        if batches:
            total_batches = len(batches)
            for batch in batches:
                payload = tuple(
                    node.to_payload()
                    for node in batch
                )
                response = Message(
                    Locations(request_id, total_batches, payload),
                    node,
                )
                await self.message_dispatcher.send_message(response)
                await self.events.sent_locations.trigger(response)
            return total_batches
        else:
            response = Message(
                Locations(request_id, 1, ()),
                node,
            )
            await self.message_dispatcher.send_message(response)
            await self.events.sent_locations.trigger(response)
            return 1

    async def send_retrieve(self,
                            node: Node,
                            *,
                            key: bytes) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(
            Retrieve(request_id, key),
            node,
        )
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_retrieve.trigger(message)
        return request_id

    async def send_chunks(self,
                          node: Node,
                          *,
                          request_id: int,
                          data: bytes) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        if not data:
            response = Message(
                Chunk(request_id, 1, 0, b''),
                node,
            )
            self.logger.debug("Sending Chunks with empty data to %s", node)
            await self.message_dispatcher.send_message(response)
            await self.events.sent_chunk.trigger(response)
            return 1

        all_chunks = split_data_to_chunks(CHUNK_MAX_SIZE, data)
        total_chunks = len(all_chunks)
        self.logger.debug("Sending %d chuncks for data payload to %s", total_chunks, node)

        for index, chunk in enumerate(all_chunks):
            response = Message(
                Chunk(request_id, total_chunks, index, chunk),
                node,
            )
            await self.message_dispatcher.send_message(response)
            await self.events.sent_chunk.trigger(response)

        return total_chunks

    #
    # Send Messages: Skip Graph
    #
    async def send_graph_get_introduction(self, node: Node) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(GraphGetIntroduction(request_id), node)
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_graph_get_introduction.trigger(message)
        return request_id

    async def send_graph_introduction(self,
                                      node: Node,
                                      *,
                                      request_id: int,
                                      graph_nodes: Collection[SGNodeAPI],
                                      ) -> None:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        graph_nodes_payload = tuple(
            SkipGraphNode.from_sg_node(sg_node) for sg_node in graph_nodes
        )
        # TODO: ensure payload size is within bounds
        message = Message(GraphIntroduction(request_id, graph_nodes_payload), node)
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_graph_introduction.trigger(message)

    async def send_graph_get_node(self, node: Node, *, key: Key) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(GraphGetNode(request_id, graph_key_to_content_key(key)), node)
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_graph_get_node.trigger(message)
        return request_id

    async def send_graph_node(self,
                              node: Node,
                              *,
                              request_id: int,
                              sg_node: Optional[SGNodeAPI]) -> None:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        payload: Optional[SkipGraphNode]
        if sg_node is None:
            payload = None
        else:
            payload = SkipGraphNode.from_sg_node(sg_node)
        message = Message(GraphNode(request_id, payload), node)
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_graph_node.trigger(message)

    async def send_graph_insert(self,
                                node: Node,
                                *,
                                key: Key) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)

        message = Message(
            GraphInsert(request_id, graph_key_to_content_key(key)),
            node,
        )
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_graph_insert.trigger(message)
        return request_id

    async def send_graph_inserted(self, node: Node, *, request_id: int) -> None:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        message = Message(GraphInserted(request_id), node)
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_graph_inserted.trigger(message)

    async def send_graph_delete(self,
                                node: Node,
                                *,
                                key: Key) -> int:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)

        message = Message(
            GraphDelete(request_id, graph_key_to_content_key(key)),
            node,
        )
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_graph_delete.trigger(message)
        return request_id

    async def send_graph_deleted(self, node: Node, *, request_id: int) -> None:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        message = Message(GraphDeleted(request_id), node)
        self.logger.debug("Sending %s", message)
        await self.message_dispatcher.send_message(message)
        await self.events.sent_graph_deleted.trigger(message)

    #
    # Request/Response
    #
    async def ping(self, node: Node) -> MessageAPI[Pong]:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(Ping(request_id), node)
        async with self.message_dispatcher.subscribe_request(message, Pong) as subscription:
            await self.events.sent_ping.trigger(message)
            return await subscription.receive()

    async def find_nodes(self, node: Node, *, distance: int) -> Tuple[MessageAPI[FoundNodes], ...]:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(FindNodes(request_id, distance), node)
        await self.events.sent_find_nodes.trigger(message)
        return await self._do_request_with_multi_response(message, FoundNodes)

    async def advertise(self, node: Node, *, key: bytes, who: Node) -> MessageAPI[Ack]:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(Advertise(request_id, key, who.to_payload()), node)
        async with self.message_dispatcher.subscribe_request(message, Ack) as subscription:
            await self.events.sent_advertise.trigger(message)
            return await subscription.receive()

    async def locate(self, node: Node, *, key: bytes) -> Tuple[MessageAPI[Locations], ...]:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(Locate(request_id, key), node)
        await self.events.sent_locate.trigger(message)
        return await self._do_request_with_multi_response(message, Locations)

    async def retrieve(self, node: Node, *, key: bytes) -> Tuple[MessageAPI[Chunk], ...]:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(Retrieve(request_id, key), node)
        responses = await self._do_request_with_multi_response(message, Chunk)
        await self.events.sent_retrieve.trigger(message)
        return tuple(sorted(responses, key=lambda response: response.payload.index))

    async def get_graph_introduction(self, node: Node) -> MessageAPI[GraphIntroduction]:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(GraphGetIntroduction(request_id), node)
        async with self.message_dispatcher.subscribe_request(message, GraphIntroduction) as subscription:  # noqa: E501
            await self.events.sent_graph_get_introduction.trigger(message)
            return await subscription.receive()

    async def get_graph_node(self, node: Node, *, key: Key) -> MessageAPI[GraphNode]:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(GraphGetNode(request_id, graph_key_to_content_key(key)), node)
        async with self.message_dispatcher.subscribe_request(message, GraphNode) as subscription:  # noqa: E501
            await self.events.sent_graph_get_node.trigger(message)
            return await subscription.receive()

    async def graph_insert(self,
                           node: Node,
                           *,
                           key: Key,
                           ) -> MessageAPI[GraphInserted]:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(GraphInsert(request_id, graph_key_to_content_key(key)), node)
        async with self.message_dispatcher.subscribe_request(message, GraphInserted) as subscription:  # noqa: E501
            await self.events.sent_graph_insert.trigger(message)
            return await subscription.receive()

    async def graph_delete(self,
                           node: Node,
                           *,
                           key: Key,
                           ) -> MessageAPI[GraphInserted]:
        if node.node_id == self.local_node_id:
            raise ValueError("Cannot send to self")
        request_id = self.message_dispatcher.get_free_request_id(node.node_id)
        message = Message(GraphDelete(request_id, graph_key_to_content_key(key)), node)
        async with self.message_dispatcher.subscribe_request(message, GraphDeleted) as subscription:  # noqa: E501
            await self.events.sent_graph_delete.trigger(message)
            return await subscription.receive()

    async def _do_request_with_multi_response(self,
                                              request: MessageAPI[sedes.Serializable],
                                              response_payload_type: Type[TPayload],
                                              ) -> Tuple[MessageAPI[TPayload], ...]:
        async with self.message_dispatcher.subscribe_request(request, response_payload_type) as subscription:  # noqa: E501
            responses = []
            total_messages = None
            while True:
                try:
                    response = await subscription.receive()
                except trio.EndOfChannel:
                    break

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
        if node.node_id == self.local_node_id:
            raise Exception("Cannot create session with self")

        try:
            session = self.pool.get_session(node.node_id)
        except SessionNotFound:
            session = self.pool.create_session(
                node,
                is_initiator=is_initiator,
            )
            await self.events.session_created.trigger(session)
            self.manager.run_task(self._monitor_handshake_timeout, session)

        return session

    #
    # Service APIs
    #
    async def run(self) -> None:
        # Run the subscription manager with gets fed all decoded inbound
        # messages and dispatches them to individual subscriptions.
        manager = self.manager.run_daemon_child_service(self.message_dispatcher)
        await manager.wait_started()

        listener = DatagramListener(
            self.listen_on,
            self._inbound_datagram_send_channel,
            self._outbound_datagram_receive_channel,
            events=self.events,
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
            self.manager.run_daemon_task(self._periodically_ping_sessions)
            self.logger.info(
                'Client running: %s@%s',
                humanize_node_id(self.local_node_id),
                self.listen_on,
            )

            self._ready.set()
            await self.manager.wait_finished()

    async def _listen_for_external_endpoint(self) -> None:
        async with self.events.new_external_ip.subscribe() as subscription:
            async for endpoint in subscription:
                self.logger.info("New External Endpoint: %s", endpoint)
                self._external_endpoint = endpoint

    async def _periodically_ping_sessions(self) -> None:
        async for _ in every(SESSION_IDLE_TIMEOUT):
            for session in self.pool.get_idle_sesssions():
                if not session.is_handshake_complete:
                    continue

                try:
                    with trio.fail_after(PING_TIMEOUT):
                        await self.ping(session.remote_node)
                except trio.TooSlowError:
                    self.logger.debug('Detected unresponsive idle session: %s', session)
                    self.pool.remove_session(session.session_id)
                    await self.events.session_idle.trigger(session)

    async def _monitor_handshake_timeout(self, session: SessionAPI) -> None:
        await trio.sleep(HANDSHAKE_TIMEOUT)
        if not session.is_handshake_complete:
            if self.pool.remove_session(session.session_id):
                self.logger.info("Detected timed out handshake: %s", session)
                await self.events.handshake_timeout.trigger(session)

    async def _handle_outbound_messages(self,
                                        receive_channel: trio.abc.ReceiveChannel[MessageAPI[sedes.Serializable]],  # noqa: E501
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

    async def _handle_session_packet(self, datagram: Datagram) -> None:
        packet = decode_packet(datagram.data)
        remote_node_id = recover_source_id_from_tag(packet.tag, self.local_node_id)
        remote_node = Node(remote_node_id, datagram.endpoint)
        session = await self._get_or_create_session(
            remote_node,
            is_initiator=False,
        )

        try:
            await session.handle_inbound_packet(packet)
        except (DecryptionError, CorruptSession):
            self.pool.remove_session(session.session_id)
            self.logger.debug('Removed defunkt session: %s', session)

            fresh_session = await self._get_or_create_session(
                remote_node,
                is_initiator=False,
            )
            # Now try again with a fresh session
            try:
                await fresh_session.handle_inbound_packet(packet)
            except DecryptionError:
                self.pool.remove_session(fresh_session.session_id)
                self.logger.debug(
                    'Unable to read packet after resetting session: %s',
                    fresh_session,
                )

    async def _handle_inbound_datagrams(self, receive_channel: trio.abc.ReceiveChannel[Datagram],
                                        ) -> None:
        try:
            async with trio.open_nursery() as nursery:
                async with receive_channel:
                    async for datagram in receive_channel:
                        nursery.start_soon(self._handle_session_packet, datagram)
                        self.logger.debug('dispatched inbound datagram %s', datagram)
        except trio.BrokenResourceError:
            pass
