import collections
import ipaddress
import pathlib
import secrets
import socket
import tempfile
from typing import Deque

from eth_keys import keys

try:
    import factory
except ImportError as err:
    raise ImportError(
        'The `factory-boy` library is required to use the `alexandria.tools.factories` module'
    ) from err


from alexandria._utils import public_key_to_node_id
from alexandria.abc import Endpoint, Node
from alexandria.app import Application
from alexandria.client import Client
from alexandria.config import KademliaConfig, StorageConfig
from alexandria.constants import (
    KEY_BYTE_SIZE,
    KADEMLIA_LOOKUP_INTERVAL,
    KADEMLIA_PING_INTERVAL,
    KADEMLIA_ANNOUNCE_INTERVAL,
)
from alexandria.durable_db import DurableDB


def _mk_private_key_bytes() -> bytes:
    return secrets.token_bytes(KEY_BYTE_SIZE)


class PrivateKeyFactory(factory.Factory):  # type: ignore
    class Meta:
        model = keys.PrivateKey

    private_key_bytes = factory.LazyFunction(_mk_private_key_bytes)


def _mk_public_key_bytes() -> bytes:
    return bytes(PrivateKeyFactory().public_key.to_bytes())


class PublicKeyFactory(factory.Factory):  # type: ignore
    class Meta:
        model = keys.PublicKey

    public_key_bytes = factory.LazyFunction(_mk_public_key_bytes)


PORT_CACHE: Deque[int] = collections.deque()
PORT_CACHE_SIZE = 256


def get_open_port() -> int:
    while True:
        port = _get_open_port()
        if port in PORT_CACHE:
            continue
        else:
            break
    if port not in PORT_CACHE:
        PORT_CACHE.append(port)
        while len(PORT_CACHE) > PORT_CACHE_SIZE:
            PORT_CACHE.popleft()

    return port


def _get_open_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return int(port)


class EndpointFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Endpoint

    ip_address = ipaddress.IPv4Address('127.0.0.1')
    port = factory.LazyFunction(get_open_port)


class NodeFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Node

    node_id = factory.LazyFunction(lambda: public_key_to_node_id(PublicKeyFactory()))
    endpoint = factory.SubFactory(EndpointFactory)


class ClientFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Client

    private_key = factory.SubFactory(PrivateKeyFactory)
    listen_on = factory.SubFactory(EndpointFactory)


class StorageConfigFactory(factory.Factory):  # type: ignore
    class Meta:
        model = StorageConfig

    ephemeral_storage_size = 4096
    ephemeral_index_size: int = 1024
    cache_storage_size: int = 1024
    cache_index_size: int = 1024


class KademliaConfigFactory(factory.Factory):  # type: ignore
    class Meta:
        model = KademliaConfig

    LOOKUP_INTERVAL = factory.LazyFunction(lambda: KADEMLIA_LOOKUP_INTERVAL)
    PING_INTERVAL: int = factory.LazyFunction(lambda: KADEMLIA_PING_INTERVAL)
    ANNOUNCE_INTERVAL: int = factory.LazyFunction(lambda: KADEMLIA_ANNOUNCE_INTERVAL)

    storage_config = factory.SubFactory(StorageConfigFactory)


class DurableDBFactory(factory.Factory):  # type: ignore
    class Meta:
        model = DurableDB

    db_path = factory.LazyFunction(lambda: pathlib.Path(tempfile.TemporaryDirectory().name))


class ApplicationFactory(factory.Factory):  # type: ignore
    class Meta:
        model = Application

    bootnodes = factory.LazyFunction(lambda: tuple())
    private_key = factory.SubFactory(PrivateKeyFactory)
    listen_on = factory.SubFactory(EndpointFactory)
    config = factory.SubFactory(KademliaConfigFactory)
    durable_db = factory.SubFactory(DurableDBFactory)
