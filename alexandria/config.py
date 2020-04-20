import argparse
from typing import Any, Iterator, NamedTuple, Tuple

from eth_utils import to_dict

from alexandria.constants import (
    KADEMLIA_ANNOUNCE_CONCURRENCY,
    KADEMLIA_ANNOUNCE_INTERVAL,
    KADEMLIA_PING_INTERVAL,
    KADEMLIA_LOOKUP_INTERVAL,
    MEGABYTE,
)


@to_dict
def _storage_config_init_kwargs_from_cli_args(args: argparse.Namespace,
                                              ) -> Iterator[Tuple[str, Any]]:
    if args.ephemeral_db_size is not None:
        yield ('ephemeral_storage_size', args.ephemeral_db_size)
    if args.ephemeral_index_size is not None:
        yield ('ephemeral_index_size', args.ephemeral_index_size)
    if args.cache_db_size is not None:
        yield ('cache_storage_size', args.cache_db_size)
    if args.cache_index_size is not None:
        yield ('cache_index_size', args.cache_index_size)


class StorageConfig(NamedTuple):
    # number of bytes
    ephemeral_storage_size: int = 100 * MEGABYTE
    # number of index entries (fixed size per entry)
    ephemeral_index_size: int = 1000000

    # number of bytes
    cache_storage_size: int = 10 * MEGABYTE
    # number of index entries (fixed size per entry)
    cache_index_size: int = 100000

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> 'StorageConfig':
        return cls(**_storage_config_init_kwargs_from_cli_args(args))


@to_dict
def _kademlia_config_init_kwargs_from_cli_args(args: argparse.Namespace,
                                               ) -> Iterator[Tuple[str, Any]]:
    if args.can_initialize_network_skip_graph:
        yield ('can_initialize_network_skip_graph', True)
    if args.lookup_interval is not None:
        yield ('LOOKUP_INTERVAL', args.lookup_interval)
    if args.ping_interval is not None:
        yield ('PING_INTERVAL', args.ping_interval)
    if args.announce_interval is not None:
        yield ('ANNOUNCE_INTERVAL', args.announce_interval)

    yield ('storage_config', StorageConfig.from_args(args))


class KademliaConfig(NamedTuple):
    LOOKUP_INTERVAL: int = KADEMLIA_LOOKUP_INTERVAL
    PING_INTERVAL: int = KADEMLIA_PING_INTERVAL
    ANNOUNCE_INTERVAL: int = KADEMLIA_ANNOUNCE_INTERVAL
    ANNOUNCE_CONCURRENCY: int = KADEMLIA_ANNOUNCE_CONCURRENCY

    storage_config: StorageConfig = StorageConfig()

    can_initialize_network_skip_graph: bool = False

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> 'KademliaConfig':
        return cls(**_kademlia_config_init_kwargs_from_cli_args(args))


DEFAULT_CONFIG = KademliaConfig()
