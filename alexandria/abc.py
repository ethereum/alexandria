from abc import ABC, abstractmethod
import ipaddress
from typing import Tuple

from eth_keys import keys
from async_service import ServiceAPI

from .typing import NodeID


class HandshakeParticipantAPI(ABC):
    @abstractmethod
    def __init__(self,
                 is_initiator: bool,
                 local_private_key: bytes,
                 remote_node_id: NodeID,
                 ) -> None:
        ...

    @property
    @abstractmethod
    def first_packet_to_send(self) -> Packet:
        """The first packet we have to send the peer."""
        ...

    @abstractmethod
    def is_response_packet(self, packet: Packet) -> bool:
        """Check if the given packet is the response we need to complete the handshake."""
        ...

    @abstractmethod
    def complete_handshake(self, response_packet: Packet) -> HandshakeResult:
        """Complete the handshake using a response packet received from the peer."""
        ...

    @property
    @abstractmethod
    def is_initiator(self) -> bool:
        """`True` if the handshake was initiated by us, `False` if it was initiated by the peer."""
        ...

    @property
    @abstractmethod
    def local_private_key(self) -> bytes:
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


class NodeAPI(ABC):
    id: NodeID
    address: ipaddress.IPv4Address
    port: int

    @property
    @abstractmethod
    def public_key(self) -> keys.PublicKey:
        ...


class ClientAPI(ServiceAPI):
    @abstractmethod
    async def handshake(self, other: NodeAPI) -> None:
        ...


class NodeDatabaseAPI(ABC):
    @abstractmethod
    async def get_bootnodes(self) -> Tuple[NodeID, ...]:
        ...
