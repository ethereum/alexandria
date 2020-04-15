import bisect
import collections
import functools
import time
import trio
from typing import Any, Deque, Dict, Generic

from alexandria.abc import TimeQueueAPI, TKey


@functools.total_ordering
class QueueItem(Generic[TKey]):
    def __init__(self,
                 key: TKey,
                 queued_at: float) -> None:
        self.key = key
        self.queued_at = queued_at

    def __eq__(self, other: Any) -> bool:
        if type(self) is type(other):
            return self.key == other.key  # type: ignore
        raise TypeError("Type mismatch")

    def __lt__(self, other: Any) -> bool:
        if type(self) is type(other):
            return self.queued_at < other.queued_at  # type: ignore
        raise TypeError("Type mismatch")


class TimeQueue(TimeQueueAPI[TKey]):
    _queue: Deque[QueueItem[TKey]]
    _key_queue_times: Dict[TKey, float]

    def __init__(self, incubation_seconds: int) -> None:
        self._queue = collections.deque()
        self._key_queue_times = {}
        self._has_content = trio.Event()
        self._incubation_seconds = incubation_seconds

    def enqueue(self, key: TKey, queue_at: float = None) -> None:
        if queue_at is None:
            queue_at = time.monotonic()

        if key in self._key_queue_times:
            current_queue_at = self._key_queue_times[key]
            if current_queue_at <= queue_at:
                # The item currently in the queue is queued to come up sooner
                # than this one so discard it
                return
            else:
                # The item in the queue is older than this one so evict the
                # current one to be replaced by this one.
                self._key_queue_times.pop(key)
                current_queue_index = bisect.bisect_left(
                    self._queue,
                    QueueItem(key, current_queue_at),
                )
                current_queue_item = self._queue[current_queue_index]
                self._queue.remove(current_queue_item)
                if current_queue_item.key != key:
                    raise Exception("Invariant")

        bisect.insort_right(self._queue, QueueItem(key, queue_at))
        self._key_queue_times[key] = queue_at
        self._has_content.set()

    def get_wait_time(self) -> float:
        delta = time.monotonic() - self._queue[0].queued_at
        return max(0.0, self._incubation_seconds - delta)

    async def pop_next(self) -> TKey:
        await self._has_content.wait()

        delay = self.get_wait_time()
        if delay > 0:
            await trio.sleep(delay)

        to_advertise = self._queue.popleft()
        self._key_queue_times.pop(to_advertise.key)

        if len(self._queue) == 0:
            self._has_content.set()
            self._has_content = trio.Event()

        return to_advertise.key
