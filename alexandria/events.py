import logging
from typing import Any, AsyncIterator, Awaitable, Generator, Set

from async_generator import asynccontextmanager
import trio

from alexandria.abc import Endpoint, SessionAPI  # noqa: F401
from alexandria.abc import (
    Datagram,
    EventAPI,
    EventsAPI,
    MessageAPI,
    TAwaitable,
    TEventPayload,
)
from alexandria.payloads import (
    Advertise, Ack,
    Retrieve, Chunk,
    FindNodes, FoundNodes,
    Locate, Locations,
    Ping, Pong,
    GraphGetIntroduction, GraphIntroduction,
    GraphGetNode, GraphNode,
    GraphLinkNodes, GraphLinked,
)


class ReAwaitable(Awaitable[TAwaitable]):
    _result: Generator[Any, None, TAwaitable]

    def __init__(self, awaitable: Awaitable[TAwaitable]) -> None:
        self._awaitable = awaitable

    @property
    def is_done(self) -> bool:
        return hasattr(self, '_result')

    def __await__(self) -> Generator[Any, None, TAwaitable]:
        if not self.is_done:
            self._result = self._awaitable.__await__()
        return self._result


class Event(EventAPI[TEventPayload]):
    logger = logging.getLogger('alexandria.events.Event')

    _channels: Set[trio.abc.SendChannel[TEventPayload]]

    def __init__(self, name: str) -> None:
        self.name = name
        self._lock = trio.Lock()
        self._channels = set()

    async def trigger(self, payload: TEventPayload) -> None:
        self.logger.debug('Triggering event: %s(%s)', self.name, payload)
        async with self._lock:
            for send_channel in self._channels:
                await send_channel.send(payload)

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator[trio.abc.ReceiveChannel[TEventPayload]]:
        send_channel, receive_channel = trio.open_memory_channel[TEventPayload](256)

        async with self._lock:
            self._channels.add(send_channel)

        try:
            async with receive_channel:
                yield receive_channel
        finally:
            async with self._lock:
                self._channels.remove(send_channel)


class Events(EventsAPI):
    def __init__(self) -> None:
        self.listening: EventAPI[Endpoint] = Event('listening')
        self.new_external_ip: EventAPI[Endpoint] = Event('new-external-ip')

        self.session_created: EventAPI[SessionAPI] = Event('session-created')
        self.session_idle: EventAPI[SessionAPI] = Event('session-idle')

        self.handshake_complete: EventAPI[SessionAPI] = Event('handshake-complete')
        self.handshake_timeout: EventAPI[SessionAPI] = Event('handshake-timeout')

        self.datagram_sent: EventAPI[Datagram] = Event('datagram-sent')
        self.datagram_received: EventAPI[Datagram] = Event('datagram-received')

        self.sent_ping: EventAPI[MessageAPI[Ping]] = Event('sent-Ping')
        self.sent_pong: EventAPI[MessageAPI[Pong]] = Event('sent-Pong')

        self.sent_find_nodes: EventAPI[MessageAPI[FindNodes]] = Event('sent-FindNodes')
        self.sent_found_nodes: EventAPI[MessageAPI[FoundNodes]] = Event('sent-FoundNodes')

        self.sent_advertise: EventAPI[MessageAPI[Advertise]] = Event('sent-Advertise')
        self.sent_ack: EventAPI[MessageAPI[Ack]] = Event('sent-Ack')

        self.sent_locate: EventAPI[MessageAPI[Locate]] = Event('sent-Locate')
        self.sent_locations: EventAPI[MessageAPI[Locations]] = Event('sent-Locations')

        self.sent_retrieve: EventAPI[MessageAPI[Retrieve]] = Event('sent-Retrieve')
        self.sent_chunk: EventAPI[MessageAPI[Chunk]] = Event('sent-Chunk')

        self.sent_graph_get_introduction: EventAPI[MessageAPI[GraphGetIntroduction]] = Event('sent-GraphGetIntroduction')  # noqa: E501
        self.sent_graph_introduction: EventAPI[MessageAPI[GraphIntroduction]] = Event('sent-GraphIntroduction')  # noqa: E501

        self.sent_graph_get_node: EventAPI[MessageAPI[GraphGetNode]] = Event('sent-GraphGetNode')
        self.sent_graph_node: EventAPI[MessageAPI[GraphNode]] = Event('sent-GraphNode')

        self.sent_graph_link_nodes: EventAPI[MessageAPI[GraphLinkNodes]] = Event('sent-GraphLinkNodes')  # noqa: E501
        self.sent_graph_linked: EventAPI[MessageAPI[GraphLinked]] = Event('sent-GraphLinked')
