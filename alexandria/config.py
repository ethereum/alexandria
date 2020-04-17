from typing import NamedTuple

from alexandria.constants import (
    KADEMLIA_ANNOUNCE_CONCURRENCY,
    KADEMLIA_ANNOUNCE_INTERVAL,
    KADEMLIA_PING_INTERVAL,
    KADEMLIA_LOOKUP_INTERVAL,
    MEGABYTE,
)


class StorageConfig(NamedTuple):
    # number of bytes
    ephemeral_storage_size: int = 100 * MEGABYTE
    # number of index entries (fixed size per entry)
    ephemeral_index_size: int = 1000000

    # number of bytes
    cache_storage_size: int = 10 * MEGABYTE
    # number of index entries (fixed size per entry)
    cache_index_size: int = 100000


class KademliaConfig(NamedTuple):
    LOOKUP_INTERVAL: int = KADEMLIA_LOOKUP_INTERVAL
    PING_INTERVAL: int = KADEMLIA_PING_INTERVAL
    ANNOUNCE_INTERVAL: int = KADEMLIA_ANNOUNCE_INTERVAL
    ANNOUNCE_CONCURRENCY: int = KADEMLIA_ANNOUNCE_CONCURRENCY

    storage_config: StorageConfig = StorageConfig()

    can_initialize_network_skip_graph: bool = False


DEFAULT_CONFIG = KademliaConfig()
