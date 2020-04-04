from abc import ABC, abstractmethod
import ipaddress
from typing import (
    AsyncContextManager,
    AsyncIterable,
    Awaitable,
    Collection,
    ContextManager,
    Generic,
    Iterator,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

from ssz import sedes

from async_service import ServiceAPI
from eth_keys import keys

from alexandria.payloads import Ack, Chunk, FoundNodes, Locations, Pong
from alexandria.typing import AES128Key, NodeID, Tag


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

    def __str__(self) -> str:
        return f"{self.ip_address}:{self.port}"


class Node(NamedTuple):
    node_id: NodeID
    endpoint: Endpoint

    def __str__(self) -> str:
        from alexandria._utils import humanize_node_id
        return f"{humanize_node_id(self.node_id)}@{self.endpoint}"


class EndpointDatabaseAPI:
    @abstractmethod
    def has_endpoint(self, node_id: NodeID) -> bool:
        ...

    @abstractmethod
    def get_endpoint(self, node_id: NodeID) -> Endpoint:
        ...

    @abstractmethod
    def set_endpoint(self, node_id: NodeID, endpoint: Endpoint) -> None:
        ...


class Datagram(NamedTuple):
    data: bytes
    endpoint: Endpoint

    def __str__(self) -> str:
        from eth_utils import humanize_hash
        return f"{humanize_hash(self.data)}@{self.endpoint}"  # type: ignore


TPayload = TypeVar('TPayload', bound=sedes.Serializable)


class RegistryAPI(ABC):
    @abstractmethod
    def register(self, message_id: int, payload_type: Type[sedes.Serializable]) -> None:
        ...

    @abstractmethod
    def get_sedes(self, message_id: int) -> sedes.Serializable:
        ...

    @abstractmethod
    def get_message_id(self, payload_type: Type[TPayload]) -> int:
        ...

    @abstractmethod
    def decode_payload(self, data: bytes) -> sedes.Serializable:
        ...


class MessageAPI(Generic[TPayload]):
    message_id: int
    payload: TPayload
    node: Node

    @abstractmethod
    def to_bytes(self) -> bytes:
        ...


class SessionKeys(NamedTuple):
    encryption_key: AES128Key
    decryption_key: AES128Key
    auth_response_key: AES128Key


class NetworkPacket(NamedTuple):
    packet: PacketAPI
    endpoint: Endpoint

    def __str__(self) -> str:
        return f"{self.packet} -> {self.endpoint}"

    def as_datagram(self) -> Datagram:
        from alexandria.packets import encode_packet
        return Datagram(
            data=encode_packet(self.packet),
            endpoint=self.endpoint,
        )


class SessionAPI(ABC):
    remote_node: Node
    remote_node_id: NodeID
    remote_endpoint: Endpoint

    is_initiator: bool

    @abstractmethod
    def __init__(self,
                 local_private_key: keys.PrivateKey,
                 remote_node_id: NodeID,
                 ) -> None:
        ...

    @abstractmethod
    async def handle_outbound_message(self, message: MessageAPI[sedes.Serializable]) -> None:
        ...

    @abstractmethod
    async def handle_inbound_packet(self, packet: PacketAPI) -> None:
        ...

    @abstractmethod
    async def handle_outbound_packet(self, packet: PacketAPI) -> None:
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
    def tag(self) -> Tag:
        """The tag used for message packets sent by this node to the peer."""
        ...


class PoolAPI(ABC):
    @abstractmethod
    def has_session(self, remote_node_id: NodeID) -> bool:
        ...

    @abstractmethod
    def get_session(self, remote_node_id: NodeID) -> SessionAPI:
        ...

    @abstractmethod
    def create_session(self, remote_node: Node, is_initiator: bool) -> SessionAPI:
        ...


TItem = TypeVar('TItem')


class SubscriptionAPI(ContextManager['SubscriptionAPI["TItem"]'], Awaitable[TItem]):
    @abstractmethod
    async def receive(self) -> TItem:
        ...

    @abstractmethod
    def stream(self) -> AsyncIterable[TItem]:
        ...


TAwaitable = TypeVar('TAwaitable')


class EventSubscriptionAPI(Awaitable[TAwaitable],
                           AsyncContextManager['EventSubscriptionAPI[TAwaitable]']):
    @abstractmethod
    async def receive(self) -> TAwaitable:
        ...

    @abstractmethod
    def stream(self) -> AsyncIterable[TAwaitable]:
        ...


TEventPayload = TypeVar('TEventPayload')


class EventAPI(Generic[TEventPayload]):
    @abstractmethod
    async def trigger(self, payload: TEventPayload) -> None:
        ...

    @abstractmethod
    def subscribe(self) -> EventSubscriptionAPI[TEventPayload]:
        ...


class EventsAPI(ABC):
    new_session: EventAPI[SessionAPI]
    listening: EventAPI[Endpoint]
    handshake_complete: EventAPI[SessionAPI]


class MessageDispatcherAPI(ServiceAPI):
    #
    # Utility
    #
    @abstractmethod
    def get_free_request_id(self, node_id: NodeID) -> int:
        ...

    #
    # Message Sending
    #
    @abstractmethod
    async def send_message(self, message: MessageAPI[sedes.Serializable]) -> None:
        ...

    #
    # Subscriptions
    #
    @abstractmethod
    def subscribe(self, payload_type: Type[TPayload]) -> SubscriptionAPI[MessageAPI[TPayload]]:
        ...

    @abstractmethod
    def subscribe_request(self,
                          message: MessageAPI[sedes.Serializable],
                          response_payload_type: Type[TPayload],
                          ) -> SubscriptionAPI[MessageAPI[TPayload]]:
        ...


class ClientAPI(ServiceAPI):
    public_key: keys.PublicKey
    local_node: Node
    local_node_id: NodeID
    listen_on: Endpoint

    events: EventsAPI
    message_dispatcher: MessageDispatcherAPI
    pool: PoolAPI

    #
    # Singular Message Sending
    #
    @abstractmethod
    async def send_ping(self, node: Node) -> int:
        ...

    @abstractmethod
    async def send_pong(self, node: Node, *, request_id: int) -> None:
        ...

    @abstractmethod
    async def send_find_nodes(self, node: Node, *, distance: int) -> int:
        ...

    @abstractmethod
    async def send_found_nodes(self,
                               node: Node,
                               *,
                               request_id: int,
                               found_nodes: Sequence[Node]) -> int:
        ...

    @abstractmethod
    async def send_advertise(self, node: Node, *, key: bytes, who: Node) -> int:
        ...

    @abstractmethod
    async def send_ack(self, node: Node, *, request_id: int) -> None:
        ...

    @abstractmethod
    async def send_locate(self, node: Node, *, key: bytes) -> int:
        ...

    @abstractmethod
    async def send_locations(self,
                             node: Node,
                             *,
                             request_id: int,
                             locations: Collection[Node]) -> int:
        ...

    @abstractmethod
    async def send_retrieve(self,
                            node: Node,
                            *,
                            key: bytes) -> int:
        ...

    @abstractmethod
    async def send_chunks(self,
                          node: Node,
                          *,
                          request_id: int,
                          data: bytes) -> int:
        ...

    #
    # Request/Response
    #
    @abstractmethod
    async def ping(self, node: Node) -> MessageAPI[Pong]:
        ...

    @abstractmethod
    async def find_nodes(self, node: Node, *, distance: int) -> Tuple[MessageAPI[FoundNodes], ...]:
        ...

    @abstractmethod
    async def advertise(self, node: Node, *, key: bytes, who: Node) -> MessageAPI[Ack]:
        ...

    @abstractmethod
    async def locate(self, node: Node, *, key: bytes) -> Tuple[MessageAPI[Locations], ...]:
        ...

    @abstractmethod
    async def retrieve(self, node: Node, *, key: bytes) -> Tuple[MessageAPI[Chunk], ...]:
        ...


class RoutingTableStats(NamedTuple):
    total_nodes: int
    bucket_size: int
    num_buckets: int
    full_buckets: Tuple[int, ...]
    num_in_replacement_cache: int


class RoutingTableAPI(Collection[NodeID]):
    bucket_size: int
    center_node_id: NodeID

    @abstractmethod
    def get_stats(self) -> RoutingTableStats:
        ...

    @abstractmethod
    def update(self, node_id: NodeID) -> Optional[NodeID]:
        ...

    @abstractmethod
    def update_bucket_unchecked(self, node_id: NodeID) -> None:
        ...

    @abstractmethod
    def remove(self, node_id: NodeID) -> None:
        ...

    @abstractmethod
    def get_nodes_at_log_distance(self, log_distance: int) -> Tuple[NodeID, ...]:
        ...

    @property
    @abstractmethod
    def is_empty(self) -> bool:
        ...

    @abstractmethod
    def get_least_recently_updated_log_distance(self) -> int:
        ...

    @abstractmethod
    def iter_nodes_around(self, reference_node_id: NodeID) -> Iterator[NodeID]:
        ...


class NetworkAPI(ABC):
    @abstractmethod
    async def single_lookup(self, node: Node, distance: int) -> Tuple[Node, ...]:
        ...

    @abstractmethod
    async def iterative_lookup(self, target_id: NodeID) -> Tuple[Node, ...]:
        ...

    @abstractmethod
    async def verify_and_add(self, node: Node) -> None:
        ...

    @abstractmethod
    async def bond(self, node: Node) -> None:
        ...
