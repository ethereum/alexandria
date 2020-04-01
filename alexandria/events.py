import logging
from typing import Awaitable, Optional, Set, Type
from types import TracebackType

import trio

from alexandria.abc import SessionAPI, EventsAPI, EventSubscriptionAPI, TAwaitable


class ReAwaitable(Awaitable[TAwaitable]):
    _result: TAwaitable

    def __init__(self, awaitable: Awaitable[TAwaitable]) -> None:
        self._awaitable = awaitable

    @property
    def is_done(self) -> bool:
        return hasattr(self, '_result')

    def __await__(self) -> TAwaitable:
        if not self.is_done:
            self._result = self._awaitable.__await__()
        return self._result


class EventSubscription(EventSubscriptionAPI[TAwaitable]):
    def __init__(self,
                 on_enter: Awaitable[None],
                 get_result: Awaitable[TAwaitable],
                 on_exit: Awaitable[None],
                 ) -> None:
        self._on_enter = on_enter
        self._get_result = ReAwaitable(get_result)
        self._on_exit = on_exit

    def __await__(self) -> TAwaitable:
        return self._do_await.__await__()

    async def _do_await(self) -> TAwaitable:
        async with self:
            return await self._get_result

    async def __aenter__(self) -> Awaitable[TAwaitable]:
        await trio.hazmat.checkpoint()
        await self._on_enter
        return self._get_result

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        traceback: Optional[TracebackType],
                        ) -> None:
        if exc_type is None and not self._get_result.is_done:
            await self._get_result
        await self._on_exit


class Events(EventsAPI):
    logger = logging.getLogger('alexandra.client.Events')
    _new_session_send_channels: Set[trio.abc.SendChannel[SessionAPI]]

    def __init__(self) -> None:
        self._new_session_send_channels = set()
        self._new_session_lock = trio.Lock()

    async def new_session(self, session: SessionAPI) -> None:
        self.logger.warning('events:NEW_CONNECTION.triggering: %s', id(self))
        async with self._new_session_lock:
            for send_channel in self._new_session_send_channels:
                await send_channel.send(session)

    def wait_new_session(self) -> EventSubscription[SessionAPI]:
        send_channel, receive_channel = trio.open_memory_channel[SessionAPI](0)

        async def on_enter():
            async with self._new_session_lock:
                self._new_session_send_channels.add(send_channel)

        async def get_result():
            async with receive_channel:
                return await receive_channel.receive()

        async def on_exit():
            async with self._new_session_lock:
                self._new_session_send_channels.remove(send_channel)

        return EventSubscription(on_enter(), get_result(), on_exit())
