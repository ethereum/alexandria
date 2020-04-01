import hashlib
import itertools
from typing import AsyncGenerator, Optional

from eth_keys import keys
import trio

from eth_utils import humanize_hash
from alexandria.typing import NodeID


def humanize_node_id(node_id: NodeID):
    node_id_bytes = node_id.to_bytes(32, 'big')
    return humanize_hash(node_id_bytes)


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def public_key_to_node_id(public_key: keys.PublicKey) -> NodeID:
    return NodeID(int.from_bytes(sha256(public_key.to_bytes()), 'big'))


async def every(interval: float,
                initial_delay: float = 0,
                ) -> AsyncGenerator[float, Optional[float]]:
    """Generator used to perform a task in regular intervals.

    The generator will attempt to yield at a sequence of target times, defined as
    `start_time + initial_delay + N * interval` seconds where `start_time` is trio's current time
    at instantiation of the generator and `N` starts at `0`. The target time is also the value that
    is yielded.

    If at a certain iteration the target time has already passed, the generator will yield
    immediately (with a checkpoint in between). The yield value is still the target time.

    The generator accepts an optional send value which will delay the next and all future
    iterations of the generator by that amount.
    """
    start_time = trio.current_time()
    undelayed_yield_times = (
        start_time + interval * iteration for iteration in itertools.count()
    )
    delay = initial_delay

    for undelayed_yield_time in undelayed_yield_times:
        yield_time = undelayed_yield_time + delay
        await trio.sleep_until(yield_time)

        additional_delay = yield yield_time
        if additional_delay is not None:
            delay += additional_delay
