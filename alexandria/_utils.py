import hashlib
import itertools
import math
from typing import AsyncGenerator, Iterable, Optional

from eth_keys import keys
from eth_typing import Hash32
import trio

from eth_utils import encode_hex, humanize_hash, to_tuple, int_to_big_endian, big_endian_to_int
from alexandria.typing import Key, NodeID


def node_id_to_hex(node_id: NodeID) -> str:
    return encode_hex(node_id.to_bytes(32, 'big'))


def content_key_to_graph_key(key: bytes) -> Key:
    return Key(big_endian_to_int(key))


def graph_key_to_content_key(key: Key) -> bytes:
    return int_to_big_endian(key)


def humanize_node_id(node_id: NodeID) -> str:
    node_id_bytes = node_id.to_bytes(32, 'big')
    return humanize_hash(Hash32(node_id_bytes))


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def content_key_to_node_id(key: bytes) -> NodeID:
    return NodeID(int.from_bytes(sha256(key), 'big'))


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


@to_tuple
def split_data_to_chunks(chunk_size: int, data: bytes) -> Iterable[bytes]:
    num_chunks = int(math.ceil(len(data) / chunk_size))
    data_view = memoryview(data)
    for chunk_number in range(num_chunks):
        yield data_view[chunk_number * chunk_size:chunk_number * chunk_size + chunk_size]
