import re
import ipaddress

from urllib import parse as urlparse

from eth_utils import ValidationError


def validate_node_uri(enode: str, require_ip: bool = False) -> None:
    try:
        parsed = urlparse.urlparse(enode)
    except ValueError as e:
        raise ValidationError(str(e))

    if parsed.scheme != 'node' or not parsed.username:
        raise ValidationError('node string must be of the form "node://nodeid@ip:port"')

    if not re.match('^[0-9a-fA-F]{64}$', parsed.username):
        raise ValidationError('public key must be a 64-character hex string')

    try:
        ip = ipaddress.ip_address(parsed.hostname)
    except ValueError as e:
        raise ValidationError(str(e))

    if require_ip and ip in (ipaddress.ip_address('0.0.0.0'), ipaddress.ip_address('::')):
        raise ValidationError('A concrete IP address must be specified')

    try:
        # this property performs a check that the port is in range
        parsed.port
    except ValueError as e:
        raise ValidationError(str(e))
