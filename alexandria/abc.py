import enum
from abc import ABC, abstractmethod
import ipaddress
import operator
from typing import (
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Collection,
    Deque,
    Dict,
    Generic,
    Iterator,
    KeysView,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Sized,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from urllib import parse as urlparse
import uuid

from async_service import ServiceAPI
from eth_keys import keys
from eth_typing import HexStr
from eth_utils import to_int, remove_0x_prefix
from ssz import sedes
import trio

from alexandria.payloads import (
    Advertise, Ack,
    Retrieve, Chunk,
    FindNodes, FoundNodes,
    Locate, Locations,
    Ping, Pong,
    GraphGetIntroduction, GraphIntroduction,
    GraphGetNode, GraphNode,
    GraphLinkNodes, GraphLinked,
)
from alexandria.typing import Key, AES128Key, NodeID, Tag


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

    @property
    def node_uri(self) -> str:
        from alexandria._utils import node_id_to_hex
        node_id_as_hex = remove_0x_prefix(HexStr(node_id_to_hex(self.node_id)))

        return f'node://{node_id_as_hex}@{self.endpoint}'

    @classmethod
    def from_node_uri(cls, uri: str) -> 'Node':
        from alexandria.validation import validate_node_uri

        validate_node_uri(uri)  # Be no more permissive than the validation
        parsed = urlparse.urlparse(uri)
        if parsed.username is None:
            raise Exception("Unreachable code path")
        node_id = NodeID(to_int(hexstr=parsed.username))
        if parsed.port is None:
            raise Exception("Unreachable code path")
        endpoint = Endpoint(ipaddress.IPv4Address(parsed.hostname), parsed.port)
        return cls(node_id, endpoint)

    def to_payload(self) -> Tuple[NodeID, bytes, int]:
        return (
            self.node_id,
            self.endpoint.ip_address.packed,
            self.endpoint.port,
        )

    @classmethod
    def from_payload(cls, payload: Tuple[NodeID, bytes, int]) -> 'Node':
        node_id, ip_address, port = payload
        return cls(
            node_id,
            Endpoint(ipaddress.IPv4Address(ip_address), port),
        )


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
    remote_public_key: keys.PublicKey

    is_initiator: bool
    last_message_at: float

    session_id: uuid.UUID

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
    def get_idle_sesssions(self) -> Tuple[SessionAPI, ...]:
        ...

    @abstractmethod
    def has_session(self, remote_node_id: NodeID) -> bool:
        ...

    @abstractmethod
    def get_session(self, remote_node_id: NodeID) -> SessionAPI:
        ...

    @abstractmethod
    def create_session(self, remote_node: Node, is_initiator: bool) -> SessionAPI:
        ...

    @abstractmethod
    def remove_session(self, session_id: uuid.UUID) -> bool:
        ...


TItem = TypeVar('TItem')


TAwaitable = TypeVar('TAwaitable')


TEventPayload = TypeVar('TEventPayload')


class EventAPI(Generic[TEventPayload]):
    name: str

    @abstractmethod
    async def trigger(self, payload: TEventPayload) -> None:
        ...

    @abstractmethod
    def subscribe(self) -> AsyncContextManager[trio.abc.ReceiveChannel[TEventPayload]]:
        ...


class EventsAPI(ABC):
    listening: EventAPI[Endpoint]
    new_external_ip: EventAPI[Endpoint]

    session_created: EventAPI[SessionAPI]
    session_idle: EventAPI[SessionAPI]

    handshake_complete: EventAPI[SessionAPI]
    handshake_timeout: EventAPI[SessionAPI]

    datagram_sent: EventAPI[Datagram]
    datagram_received: EventAPI[Datagram]

    sent_ping: EventAPI[MessageAPI[Ping]]
    sent_pong: EventAPI[MessageAPI[Pong]]

    sent_find_nodes: EventAPI[MessageAPI[FindNodes]]
    sent_found_nodes: EventAPI[MessageAPI[FoundNodes]]

    sent_advertise: EventAPI[MessageAPI[Advertise]]
    sent_ack: EventAPI[MessageAPI[Ack]]

    sent_locate: EventAPI[MessageAPI[Locate]]
    sent_locations: EventAPI[MessageAPI[Locations]]

    sent_retrieve: EventAPI[MessageAPI[Retrieve]]
    sent_chunk: EventAPI[MessageAPI[Chunk]]

    sent_graph_get_introduction: EventAPI[MessageAPI[GraphGetIntroduction]]
    sent_graph_introduction: EventAPI[MessageAPI[GraphIntroduction]]

    sent_graph_get_node: EventAPI[MessageAPI[GraphGetNode]]
    sent_graph_node: EventAPI[MessageAPI[GraphNode]]

    sent_graph_link_nodes: EventAPI[MessageAPI[GraphLinkNodes]]
    sent_graph_linked: EventAPI[MessageAPI[GraphLinked]]


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
    def subscribe(self,
                  payload_type: Type[TPayload],
                  ) -> AsyncContextManager[trio.abc.ReceiveChannel[MessageAPI[TPayload]]]:
        ...

    @abstractmethod
    def subscribe_request(self,
                          message: MessageAPI[sedes.Serializable],
                          response_payload_type: Type[TPayload],
                          ) -> AsyncContextManager[trio.abc.ReceiveChannel[MessageAPI[TPayload]]]:
        ...


class Direction(enum.IntEnum):
    left = 0
    right = 1

    @property
    def opposite(self) -> 'Direction':
        if self is Direction.left:
            return Direction.right
        elif self is Direction.right:
            return Direction.left
        else:
            raise Exception("Invariant")

    @property
    def comparison_fn(self) -> Callable[[Key, Key], bool]:
        if self is Direction.left:
            return operator.lt
        elif self is Direction.right:
            return operator.gt
        else:
            raise Exception("Invariant")


class SGNodeAPI(ABC):
    key: Key
    neighbors = Tuple[List[Key], List[Key]]
    membership_vector: NodeID

    @property
    @abstractmethod
    def max_level(self) -> int:
        ...

    @abstractmethod
    def get_neighbor(self, at_level: int, direction: Direction) -> Optional[Key]:
        ...

    @abstractmethod
    def set_neighbor(self, at_level: int, direction: Direction, key: Optional[Key]) -> None:
        ...

    @abstractmethod
    def iter_neighbors(self) -> Iterator[Tuple[int, Direction, Key]]:
        ...

    @abstractmethod
    def iter_neighbor_pairs(self) -> Iterator[Tuple[Optional[Key], Optional[Key]]]:
        ...

    @abstractmethod
    def iter_down_levels(self,
                         from_level: int,
                         direction: Direction,
                         ) -> Iterator[Tuple[int, Optional[Key]]]:
        ...


class ClientAPI(ServiceAPI):
    public_key: keys.PublicKey
    local_node: Node
    local_node_id: NodeID
    listen_on: Endpoint

    events: EventsAPI
    message_dispatcher: MessageDispatcherAPI
    pool: PoolAPI

    @property
    @abstractmethod
    def external_endpoint(self) -> Endpoint:
        ...

    @abstractmethod
    async def wait_ready(self) -> None:
        ...

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

    @abstractmethod
    async def send_graph_get_introduction(self, node: Node) -> int:
        ...

    @abstractmethod
    async def send_graph_introduction(self,
                                      node: Node,
                                      *,
                                      request_id: int,
                                      graph_nodes: Collection[SGNodeAPI],
                                      ) -> None:
        ...

    @abstractmethod
    async def send_graph_get_node(self, node: Node, *, key: Key) -> int:
        ...

    @abstractmethod
    async def send_graph_node(self,
                              node: Node,
                              *,
                              request_id: int,
                              sg_node: Optional[SGNodeAPI]) -> None:
        ...

    @abstractmethod
    async def send_graph_link_nodes(self,
                                    node: Node,
                                    *,
                                    left: Optional[Key],
                                    right: Optional[Key],
                                    level: int) -> int:
        ...

    @abstractmethod
    async def send_graph_linked(self, node: Node, *, request_id: int) -> None:
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

    @abstractmethod
    async def get_graph_introduction(self, node: Node) -> MessageAPI[GraphIntroduction]:
        ...

    @abstractmethod
    async def get_graph_node(self, node: Node, *, key: Key) -> MessageAPI[GraphNode]:
        ...

    @abstractmethod
    async def link_graph_nodes(self,
                               node: Node,
                               *,
                               left: Optional[Key],
                               right: Optional[Key],
                               level: int,
                               ) -> MessageAPI[GraphLinked]:
        ...


class RoutingTableStats(NamedTuple):
    total_nodes: int
    bucket_size: int
    num_buckets: int
    full_buckets: Tuple[int, ...]
    num_in_replacement_cache: int


class BucketInfo(NamedTuple):
    idx: int
    is_full: bool
    nodes: Tuple[NodeID, ...]
    replacement_cache: Tuple[NodeID, ...]


class RoutingTableAPI(Collection[NodeID]):
    center_node_id: NodeID
    bucket_size: int
    bucket_count: int

    buckets: Tuple[Deque[NodeID], ...]

    replacement_caches: Tuple[Deque[NodeID], ...]

    bucket_update_order: Deque[int]

    @abstractmethod
    def get_stats(self) -> RoutingTableStats:
        ...

    @abstractmethod
    def get_bucket_info(self, index: int) -> BucketInfo:
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
    async def single_lookup(self, node: Node, *, distance: int) -> Tuple[Node, ...]:
        ...

    @abstractmethod
    async def iterative_lookup(self,
                               target_id: NodeID,
                               filter_self: bool = True) -> Tuple[Node, ...]:
        ...

    @abstractmethod
    async def verify_and_add(self, node: Node) -> None:
        ...

    @abstractmethod
    async def bond(self, node: Node) -> None:
        ...

    @abstractmethod
    async def announce(self, key: bytes, who: Node) -> None:
        ...

    @abstractmethod
    async def locate(self, node: Node, *, key: bytes) -> Tuple[Node, ...]:
        ...

    @abstractmethod
    async def retrieve(self, node: Node, *, key: bytes) -> bytes:
        ...

    @abstractmethod
    async def get_content(self, key: bytes) -> bytes:
        ...

    @abstractmethod
    async def get_introduction(self, node: Node) -> Tuple[SGNodeAPI, ...]:
        ...

    @abstractmethod
    async def get_graph_node(self, node: Node, *, key: Key) -> SGNodeAPI:
        ...

    @abstractmethod
    async def get_node(self, key: Key) -> SGNodeAPI:
        ...

    @abstractmethod
    async def link_nodes(self,
                         left: Optional[Key],
                         right: Optional[Key],
                         level: int) -> None:
        ...


class ContentBundle(NamedTuple):
    key: bytes
    data: Optional[bytes]
    node_id: NodeID


class Location(NamedTuple):
    content_id: NodeID
    node_id: NodeID


class Content(NamedTuple):
    key: bytes
    data: bytes


class DurableDatabaseAPI(Sized):
    @abstractmethod
    def keys(self) -> KeysView[bytes]:
        ...

    @abstractmethod
    def has(self, key: bytes) -> bool:
        ...

    @abstractmethod
    def get(self, key: bytes) -> bytes:
        ...

    @abstractmethod
    def set(self, key: bytes, data: bytes) -> None:
        ...

    @abstractmethod
    def delete(self, key: bytes) -> None:
        ...


TKey = TypeVar('TKey')
TValue = TypeVar('TValue')


class LimitedCapacityDatabaseAPI(Sized, Generic[TKey, TValue]):
    total_capacity: int
    capacity: int

    @property
    @abstractmethod
    def has_capacity(self) -> bool:
        ...

    @abstractmethod
    def keys(self) -> KeysView[TKey]:
        ...

    @abstractmethod
    def has(self, key: TKey) -> bool:
        ...

    @abstractmethod
    def get(self, key: TKey) -> TValue:
        ...

    @abstractmethod
    def set(self, key: TKey, value: TValue) -> None:
        ...

    @abstractmethod
    def delete(self, key: TKey) -> None:
        ...


class ContentIndexAPI(Sized):
    total_capacity: int
    capacity: int

    @abstractmethod
    def get_index(self, key: NodeID) -> Tuple[NodeID, ...]:
        ...

    def has(self, key: NodeID) -> bool:
        ...

    @abstractmethod
    def add(self, location: Location) -> None:
        ...

    @abstractmethod
    def remove(self, location: Location) -> None:
        ...


class ContentStats(NamedTuple):
    durable_item_count: int
    ephemeral_db_count: int
    ephemeral_db_total_capacity: int
    ephemeral_db_capacity: int
    ephemeral_index_total_capacity: int
    ephemeral_index_capacity: int
    cache_db_count: int
    cache_db_total_capacity: int
    cache_db_capacity: int
    cache_index_total_capacity: int
    cache_index_capacity: int


class TimeQueueAPI(Generic[TKey]):
    @abstractmethod
    def enqueue(self, key: TKey, queue_at: float = None) -> None:
        ...

    @abstractmethod
    def get_wait_time(self) -> float:
        ...

    @abstractmethod
    async def pop_next(self) -> TKey:
        ...


class ContentManagerAPI(ABC):
    center_id: NodeID

    durable_db: DurableDatabaseAPI
    durable_index: Mapping[NodeID, Tuple[NodeID, ...]]

    ephemeral_db: LimitedCapacityDatabaseAPI[bytes, bytes]
    ephemeral_index: ContentIndexAPI

    cache_db: LimitedCapacityDatabaseAPI[bytes, bytes]
    cache_index: ContentIndexAPI

    @abstractmethod
    def rebuild_durable_index(self) -> None:
        ...

    @abstractmethod
    def iter_content_keys(self) -> Tuple[bytes, ...]:
        ...

    @abstractmethod
    def ingest_content(self, content: ContentBundle) -> None:
        ...

    @abstractmethod
    def has_key(self, key: bytes) -> bool:
        ...

    @abstractmethod
    def has_data(self, key: bytes) -> bool:
        ...

    @abstractmethod
    def get_content(self, key: bytes) -> bytes:
        ...

    @abstractmethod
    def get_index(self, content_id: NodeID) -> Tuple[NodeID, ...]:
        ...

    @abstractmethod
    def get_stats(self) -> ContentStats:
        ...


class GraphDatabaseAPI(ABC):
    _db: Dict[Key, SGNodeAPI]

    @abstractmethod
    def keys(self) -> KeysView[Key]:
        ...

    @abstractmethod
    def has(self, key: Key) -> bool:
        ...

    @abstractmethod
    def get(self, key: Key) -> SGNodeAPI:
        ...

    @abstractmethod
    def set(self, key: Key, node: SGNodeAPI) -> None:
        ...

    @abstractmethod
    def delete(self, key: Key) -> None:
        ...


FindResult = Union[
    Tuple[None, SGNodeAPI, None],
    Tuple[SGNodeAPI, None, None],
    Tuple[SGNodeAPI, None, SGNodeAPI],
    Tuple[None, None, SGNodeAPI],
]


class GraphAPI(ABC):
    cursor: SGNodeAPI
    db: GraphDatabaseAPI

    @property
    @abstractmethod
    def has_cursor(self) -> bool:
        ...

    @abstractmethod
    async def insert(self, key: Key, cursor: Optional[SGNodeAPI] = None) -> SGNodeAPI:
        ...

    @abstractmethod
    async def delete(self, key: Key, cursor: Optional[SGNodeAPI] = None) -> None:
        ...

    @abstractmethod
    async def find(self, key: Key, cursor: Optional[SGNodeAPI] = None) -> FindResult:
        ...

    @abstractmethod
    async def search(self, key: Key, cursor: Optional[SGNodeAPI] = None) -> SGNodeAPI:
        ...

    @abstractmethod
    def iter_keys(self,
                  start: Key = Key(0),
                  end: Optional[Key] = None) -> AsyncIterator[Key]:
        ...

    @abstractmethod
    def iter_values(self,
                    start: Key = Key(0),
                    end: Optional[Key] = None) -> AsyncIterator[SGNodeAPI]:
        ...

    @abstractmethod
    def iter_items(self,
                   start: Key = Key(0),
                   end: Optional[Key] = None) -> AsyncIterator[Tuple[Key, SGNodeAPI]]:
        ...


class KademliaAPI(ServiceAPI):
    client: ClientAPI
    network: NetworkAPI
    routing_table: RoutingTableAPI
    content_manager: ContentManagerAPI
    graph_db: GraphDatabaseAPI
    advertise_queue: TimeQueueAPI[bytes]
    graph_queue: TimeQueueAPI[Key]

    @property
    @abstractmethod
    def graph(self) -> GraphAPI:
        ...
