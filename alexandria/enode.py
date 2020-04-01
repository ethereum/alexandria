import re
import ipaddress
from typing import Type, TypeVar

from urllib import parse as urlparse

from eth_utils import (
    decode_hex,
    remove_0x_prefix,
    ValidationError,
)

from eth_keys import (
    keys,
)

from alexandria._utils import public_key_to_node_id, humanize_node_id
from alexandria.abc import Endpoint


def validate_enode_uri(enode: str, require_ip: bool = False) -> None:
    try:
        parsed = urlparse.urlparse(enode)
    except ValueError as e:
        raise ValidationError(str(e))

    if parsed.scheme != 'enode' or not parsed.username:
        raise ValidationError('enode string must be of the form "enode://public-key@ip:port"')

    if not re.match('^[0-9a-fA-F]{128}$', parsed.username):
        raise ValidationError('public key must be a 128-character hex string')

    decoded_username = decode_hex(parsed.username)

    try:
        ip = ipaddress.ip_address(parsed.hostname)
    except ValueError as e:
        raise ValidationError(str(e))

    if require_ip and ip in (ipaddress.ip_address('0.0.0.0'), ipaddress.ip_address('::')):
        raise ValidationError('A concrete IP address must be specified')

    keys.PublicKey(decoded_username)

    try:
        # this property performs a check that the port is in range
        parsed.port
    except ValueError as e:
        raise ValidationError(str(e))


TNode = TypeVar('TNode')


class Node:
    public_key: keys.PublicKey
    endpoint: Endpoint

    def __init__(self,
                 public_key: keys.PublicKey,
                 endpoint: Endpoint) -> None:
        self.public_key = public_key
        self.endpoint = endpoint
        self.node_id = public_key_to_node_id(self.public_key_to_node_id)

    def __str__(self) -> str:
        node_id_display = humanize_node_id(public_key_to_node_id(self.public_key))

        return f'{node_id_display}@{self.endpoint}'

    @property
    def enode_uri(self) -> str:
        public_key_as_hex = remove_0x_prefix(self.public_key.to_hex())

        return f'enode://{public_key_as_hex}@{self.endpoint}'

    @classmethod
    def from_enode_uri(cls: Type[TNode], uri: str) -> TNode:
        validate_enode_uri(uri)  # Be no more permissive than the validation
        parsed = urlparse.urlparse(uri)
        pubkey = keys.PublicKey(decode_hex(parsed.username))
        endpoint = Endpoint(parsed.hostname, parsed.port)
        return cls(pubkey, endpoint)
