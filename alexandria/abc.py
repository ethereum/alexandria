from abc import ABC, abstractmethod
import ipaddress
from typing import NamedTuple, Optional, Tuple, Type, TypeVar

from async_service import ServiceAPI
from eth_keys import keys
import trio

from .typing import NodeID, Tag


class PacketAPI(ABC):
    packet_id: int
    tag: Tag

    @abstractmethod
    def to_wire_bytes(self) -> bytes:
        ...

    @classmethod
    @abstractmethod
    def from_wire_bytes(cls: Type['TPacket'], data: bytes) -> 'TPacket':
        ...


TPacket = TypeVar('TPacket', bound=PacketAPI)


class Endpoint(NamedTuple):
    ip_address: ipaddress.IPv4Address
    port: int


class Datagram(NamedTuple):
    datagram: bytes
    endpoint: Endpoint


class MessageAPI(ABC):
    ...


class SessionAPI(ABC):
    @abstractmethod
    def __init__(self,
                 is_initiator: bool,
                 local_private_key: keys.PrivateKey,
                 remote_node_id: NodeID,
                 ) -> None:
        ...

    @abstractmethod
    async def handle_inbound_packet(self, packet: PacketAPI) -> Optional[PacketAPI]:
        ...

    @abstractmethod
    async def handle_outbound_packet(self, packet: PacketAPI) -> PacketAPI:
        ...

    @property
    @abstractmethod
    def is_before_handshake(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_handshake_complete(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_during_handshake(self) -> bool:
        ...

    @property
    @abstractmethod
    def is_initiator(self) -> bool:
        """`True` if the handshake was initiated by us, `False` if it was initiated by the peer."""
        ...

    @property
    @abstractmethod
    def private_key(self) -> keys.PrivateKey:
        """The static node key of this node."""
        ...

    @property
    @abstractmethod
    def local_node_id(self) -> NodeID:
        """The node id of this node."""
        ...

    @property
    @abstractmethod
    def remote_node_id(self) -> NodeID:
        """The peer's node id."""
        ...

    @property
    @abstractmethod
    def tag(self) -> Tag:
        """The tag used for message packets sent by this node to the peer."""
        ...


class ConnectionAPI(ABC):
    inbound_packet_send_channel: trio.abc.SendChannel[PacketAPI]
    outbound_packet_receive_channel: trio.abc.SendChannel[PacketAPI]

    remote_endpoint: Endpoint

    @abstractmethod
    async def wait_ready(self) -> None:
        ...


class NodeAPI(ABC):
    id: NodeID
    address: ipaddress.IPv4Address
    port: int

    @property
    @abstractmethod
    def public_key(self) -> keys.PublicKey:
        ...


class EventsAPI(ABC):
    @abstractmethod
    async def new_connection(self, connection: ConnectionAPI) -> None:
        ...

    @abstractmethod
    async def wait_new_connection(self) -> ConnectionAPI:
        ...


class ClientAPI(ServiceAPI):
    events: EventsAPI


class NodeDatabaseAPI(ABC):
    @abstractmethod
    async def get_bootnodes(self) -> Tuple[NodeID, ...]:
        ...
