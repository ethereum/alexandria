import collections
import logging
from typing import (
    AsyncIterable,
    Callable,
    ContextManager,
    Generic,
    DefaultDict,
    Optional,
    Set,
    Type,
)
from types import TracebackType

from async_service import Service
from ssz import sedes
import trio

from alexandria.abc import MessageAPI, RegistryAPI, SubscriptionAPI, TItem, TPayload
from alexandria.messages import default_registry


class Subscription(ContextManager['Subscription[TItem]'], Generic[TItem]):
    def __init__(self,
                 remove_fn: Callable[[], None],
                 receive_channel: trio.abc.ReceiveChannel[TItem],
                 ) -> None:
        self._remove_fn = remove_fn
        self._receive_channel = receive_channel

    def __enter__(self) -> 'Subscription[TItem]':
        return self

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType],
                 ) -> None:
        self._remove_fn()

    async def receive(self) -> TItem:
        return await self._receive_channel.receive()

    async def stream(self) -> AsyncIterable[TItem]:
        async with self.receive_channel:
            async for message in self._receive_channel:
                yield message


class SubscriptionManager(Service):
    logger = logging.getLogger('alexandria.subscriptions.SubscriptionManager')

    _subscriptions: DefaultDict[int, Set[trio.abc.SendChannel[MessageAPI[sedes.Serializable]]]]

    def __init__(self,
                 receive_channel: trio.abc.ReceiveChannel[MessageAPI[sedes.Serializable]],
                 message_registry: RegistryAPI = default_registry,
                 ) -> None:
        self._registry = message_registry
        self._receive_channel = receive_channel
        self._subscriptions = collections.defaultdict(set)

    async def run(self) -> None:
        self.manager.run_daemon_task(self._feed_subscriptions, self._receive_channel)

    async def _feed_subscriptions(self,
                                  receive_channel: trio.abc.ReceiveChannel[MessageAPI[sedes.Serializable]],  # noqa: E501
                                  ) -> None:
        async with receive_channel:
            async with trio.open_nursery() as nursery:
                async for message in receive_channel:
                    channels = self._subscriptions[message.message_id]
                    self.logger.debug(
                        'Handling %d subscriptions for message: %s',
                        len(channels),
                        message,
                    )
                    for send_channel in channels:
                        nursery.start_soon(send_channel.send, message)

    def subscribe(self, payload_type: Type[TPayload]) -> SubscriptionAPI[MessageAPI[TPayload]]:
        message_id = self._registry.get_message_id(payload_type)
        send_channel, receive_channel = trio.open_memory_channel[MessageAPI[TPayload]](0)
        self._subscriptions[message_id].add(send_channel)
        subscription = Subscription(
            lambda: self._subscriptions[message_id].remove(send_channel),
            receive_channel,
        )
        return subscription
