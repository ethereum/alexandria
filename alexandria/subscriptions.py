from typing import (
    AsyncIterable,
    Callable,
    ContextManager,
    Generic,
    Optional,
    Type,
)
from types import TracebackType

import trio

from alexandria.abc import TItem


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

    def __await__(self) -> TItem:
        return self.receive().__await__()

    async def receive(self) -> TItem:
        return await self._receive_channel.receive()

    async def stream(self) -> AsyncIterable[TItem]:
        async with self._receive_channel:
            async for message in self._receive_channel:
                yield message
