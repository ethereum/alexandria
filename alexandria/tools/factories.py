import ipaddress
import secrets
import socket

from eth_keys import keys

try:
    import factory
except ImportError as err:
    raise ImportError(
        'The `factory-boy` library is required to use the `alexandria.tools.factories` module'
    ) from err


from alexandria.app import Application
from alexandria.client import Client, Endpoint
from alexandria.constants import KEY_BYTE_SIZE


def _mk_private_key_bytes() -> bytes:
    return secrets.token_bytes(KEY_BYTE_SIZE)


class PrivateKeyFactory(factory.Factory):
    class Meta:
        model = keys.PrivateKey

    private_key_bytes = factory.LazyFunction(_mk_private_key_bytes)


def _mk_public_key_bytes() -> bytes:
    return PrivateKeyFactory().public_key.to_bytes()


class PublicKeyFactory(factory.Factory):
    class Meta:
        model = keys.PublicKey

    public_key_bytes = factory.LazyFunction(_mk_public_key_bytes)


def get_open_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


class EndpointFactory(factory.Factory):
    class Meta:
        model = Endpoint

    ip_address = ipaddress.IPv4Address('127.0.0.1')
    port = factory.LazyFunction(get_open_port)


class ClientFactory(factory.Factory):
    class Meta:
        model = Client

    private_key = factory.SubFactory(PrivateKeyFactory)
    listen_on = factory.SubFactory(EndpointFactory)


class ApplicationFactory(factory.Factory):
    class Meta:
        model = Application

    bootnodes = factory.LazyFunction(lambda: tuple())
    private_key = factory.SubFactory(PrivateKeyFactory)
    listen_on = factory.SubFactory(EndpointFactory)
