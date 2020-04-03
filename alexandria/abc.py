from abc import ABC, abstractmethod
import ipaddress
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterable,
    Awaitable,
    Callable,
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

from alexandria.payloads import FoundNodes, Pong
from alexandria.typing import NodeID, Tag


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

    def __str__(self):
        return f"{self.ip_address}:{self.port}"


class Node(NamedTuple):
    node_id: NodeID
    endpoint: Endpoint

    def __str__(self) -> None:
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

    def __str__(self):
        from eth_utils import humanize_hash
        return f"{humanize_hash(self.data)}@{self.endpoint}"


TPayload = TypeVar('TPayload', bound=sedes.Serializable)


class RegistryAPI(ABC):
    @abstractmethod
    def register(self, message_id: int) -> Callable[[Type[TPayload]], Type[TPayload]]:
        ...


class MessageAPI(Generic[TPayload]):
    message_id: int
    payload: TPayload
    node_id: NodeID
    endpoint: Endpoint

    @abstractmethod
    def to_bytes(self) -> bytes:
        ...


class SessionAPI(ABC):
    remote_node: Node
    remote_node_id: NodeID
    remote_endpoint: Endpoint

    @abstractmethod
    def __init__(self,
                 local_private_key: keys.PrivateKey,
                 remote_node_id: NodeID,
                 ) -> None:
        ...

    @abstractmethod
    async def handle_outbound_message(self, message: MessageAPI) -> None:
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
    def create_session(self, remote_node_id: NodeID, remote_endpoint: Endpoint) -> SessionAPI:
        ...


TItem = TypeVar('TItem')


class SubscriptionAPI(ContextManager['SubscriptionAPI["TItem"]'], Awaitable[TItem]):
    @abstractmethod
    async def receive(self) -> TItem:
        ...

    @abstractmethod
    async def stream(self) -> AsyncIterable[TItem]:
        ...


TAwaitable = TypeVar('TAwaitable', bound=Awaitable[Any])


class EventSubscriptionAPI(Awaitable[TAwaitable], AsyncContextManager[Awaitable[TAwaitable]]):
    pass


TEventPayload = TypeVar('TEventPayload')


class EventAPI(Generic[TEventPayload]):
    @abstractmethod
    async def trigger(self, payload: TEventPayload) -> None:
        ...

    @abstractmethod
    def subscribe(self) -> EventSubscriptionAPI[TEventPayload]:
        ...


class EventsAPI(ABC):
    new_session: EventAPI
    listening: EventAPI
    handshake_complete: EventAPI


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
    async def send_message(self, message: MessageAPI) -> None:
        ...

    #
    # Subscriptions
    #
    @abstractmethod
    def subscribe(self, payload_type: Type[TPayload]) -> SubscriptionAPI[MessageAPI[TPayload]]:
        ...

    @abstractmethod
    def subscribe_request(self,
                          message: MessageAPI,
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

    #
    # Request/Response
    #
    @abstractmethod
    async def ping(self, node: Node) -> Pong:
        ...

    @abstractmethod
    async def find_nodes(self, node: Node, distance: int) -> FoundNodes:
        ...


class RoutingTableStats(NamedTuple):
    total_nodes: int
    bucket_size: int
    num_buckets: int
    full_buckets: Tuple[int, ...]
    num_in_replacement_cache: int


class RoutingTableAPI(Collection[NodeID]):
    @abstractmethod
    def get_stats(self) -> RoutingTableStats:
        ...

    @abstractmethod
    def update(self, node_id: NodeID) -> NodeID:
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
