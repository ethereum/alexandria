import bisect
import collections
import functools
import itertools
import logging
from typing import (
    Any,
    Collection,
    Deque,
    Dict,
    FrozenSet,
    Iterator,
    KeysView,
    List,
    Mapping,
    Optional,
    Tuple,
)

from eth_utils import encode_hex, to_tuple
from eth_utils.toolz import groupby

from alexandria._utils import content_key_to_node_id
from alexandria.abc import (
    ContentDatabaseAPI,
    ContentIndexAPI,
    ContentManagerAPI,
    ContentStats,
    ContentBundle,
    Content,
    DurableDatabaseAPI,
    Location,
)
from alexandria.config import StorageConfig
from alexandria.typing import NodeID
from alexandria.routing_table import compute_distance


def iter_furthest_keys(center_id: NodeID, keys: Collection[bytes]) -> Iterator[bytes]:
    def sort_fn(key: bytes) -> int:
        content_id = content_key_to_node_id(key)
        return compute_distance(center_id, content_id)

    yield from sorted(keys, key=sort_fn, reverse=True)


class BaseContentDB(ContentDatabaseAPI):
    @property
    def has_capacity(self) -> bool:
        return self.capacity > 0


@functools.total_ordering
class SortableKey:
    key: bytes
    distance: int

    def __init__(self, key: bytes, distance: int) -> None:
        self.key = key
        self.distance = distance

    def __eq__(self, other: Any):
        if type(other) is not type(self):
            raise TypeError("Invalid comparison")
        return self.key == other.key

    def __lt__(self, other: Any):
        if type(other) is not type(self):
            raise TypeError("Invalid comparison")
        return self.distance < other.distance


class EphemeralDB(BaseContentDB):
    _db: Dict[bytes, bytes]
    _keys: List[SortableKey]

    def __init__(self, center_id: NodeID, capacity: int) -> None:
        self._db = {}
        self._keys_by_distance = []
        self.center_id = center_id
        self.capacity = capacity
        self.total_capacity = capacity

    def __len__(self) -> int:
        return len(self._db)

    def keys(self) -> KeysView[bytes]:
        return collections.KeysView(self._db)

    def _enforce_capacity(self) -> None:
        while self.capacity < 0:
            self.delete(self._keys_by_distance[-1].key)

    def has(self, key: bytes) -> bool:
        return key in self._db

    def get(self, key: bytes) -> bytes:
        return self._db[key]

    def set(self, content: Content) -> None:
        key, data = content
        try:
            if self.get(key) == data:
                return
            else:
                self.delete(key)
        except KeyError:
            pass

        self._db[key] = data
        content_id = content_key_to_node_id(key)
        distance = compute_distance(self.center_id, content_id)
        bisect.insort_right(self._keys_by_distance, SortableKey(key, distance))
        self.capacity -= len(data)
        self._enforce_capacity()

    def delete(self, key: bytes) -> None:
        data = self._db.pop(key)
        self.capacity += len(data)
        content_id = content_key_to_node_id(key)
        distance = compute_distance(self.center_id, content_id)
        key_index = bisect.bisect_left(self._keys_by_distance, SortableKey(key, distance))
        if self._keys_by_distance[key_index].key == key:
            self._keys_by_distance.pop(key_index)
        else:
            assert False


def iter_furthest_content_ids(center_id: NodeID,
                              content_ids: Collection[NodeID]) -> Iterator[NodeID]:
    sort_fn = functools.partial(compute_distance, center_id)

    yield from sorted(content_ids, key=sort_fn, reverse=True)


@functools.total_ordering
class SortableNodeID:
    node_id: NodeID
    distance: int

    def __init__(self, node_id: NodeID, distance: int) -> None:
        self.node_id = node_id
        self.distance = distance

    def __eq__(self, other: Any):
        if type(other) is not type(self):
            raise TypeError("Invalid comparison")
        return self.node_id == other.node_id

    def __lt__(self, other: Any):
        if type(other) is not type(self):
            raise TypeError("Invalid comparison")
        return self.distance < other.distance


class EphemeralIndex(ContentIndexAPI):
    _indices: Dict[NodeID, List[SortableNodeID]]
    _indices_by_distance = List[SortableNodeID]

    def __init__(self, center_id: NodeID, capacity: int) -> None:
        self._indices = {}
        self._indices_by_distance = []
        self.center_id = center_id
        self.capacity = capacity
        self.total_capacity = capacity

    def __len__(self) -> int:
        return sum(len(index) for index in self._indices.values())

    def _enforce_capacity(self) -> None:
        while self.capacity < 0:
            furthest_content_id = self._indices_by_distance[-1].node_id
            furthest_index = self._indices[furthest_content_id]
            furthest_location_id = furthest_index[-1].node_id
            self.remove(Location(furthest_content_id, furthest_location_id))

    def get_index(self, key: NodeID) -> Tuple[NodeID, ...]:
        index = self._indices[key]
        return tuple(item.node_id for item in index)

    def add(self, location: Location) -> None:
        content_id, location_id = location
        index: List[SortableNodeID]
        try:
            index = self._indices[content_id]
        except KeyError:
            index = []
            self._indices[content_id] = index

        content_distance = compute_distance(self.center_id, content_id)
        bisect.insort_right(
            self._indices_by_distance,
            SortableNodeID(content_id, content_distance),
        )

        location_distance = compute_distance(self.center_id, location_id)
        bisect.insort_right(
            index,
            SortableNodeID(location_id, location_distance),
        )

        self.capacity -= 1
        self._enforce_capacity()

    def remove(self, location: Location) -> None:
        content_id, location_id = location
        index = self._indices[content_id]

        location_distance = compute_distance(self.center_id, location_id)
        location_index = bisect.bisect_left(index, SortableNodeID(location_id, location_distance))
        if index[location_index].node_id != location_id:
            return

        index.pop(location_index)
        self.capacity += 1

        if len(index) == 0:
            content_distance = compute_distance(self.center_id, content_id)
            index_index = bisect.bisect_left(
                self._indices_by_distance,
                SortableNodeID(content_id, content_distance),
            )

            if self._indices_by_distance[index_index].node_id != content_id:
                raise Exception("Invariant!")
            self._indices_by_distance.pop(index_index)
            self._indices.pop(content_id)


class CacheDB(BaseContentDB):
    _records: 'collections.OrderedDict[Content]'

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.total_capacity = capacity
        self._records = collections.OrderedDict()

    def __len__(self) -> int:
        return len(self._records)

    def keys(self) -> KeysView[bytes]:
        return self._records.keys()

    def _enforce_capacity(self) -> None:
        while self.capacity < 0:
            oldest_data = self._records.popitem(last=False)
            self.capacity += len(oldest_data)

    def has(self, key: bytes) -> bool:
        return key in self._records

    def get(self, key: bytes) -> bytes:
        self._records.move_to_end(key, last=False)
        return self._records[key]

    def set(self, content: Content) -> None:
        try:
            current_data = self._records.pop(content.key)
        except KeyError:
            pass
        else:
            self.capacity += len(current_data)

        self._records[content.key] = content.data
        self.capacity -= len(content.data)
        self._enforce_capacity()

    def delete(self, key: bytes) -> None:
        data = self._records.pop(key)
        self.capacity += len(data)


class CacheIndex(ContentIndexAPI):
    _records: Deque[Location]
    _cache_indices: Mapping[NodeID, FrozenSet[NodeID]]

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.total_capacity = capacity
        self._records = collections.deque()
        self._counter = 0
        self._last_cached_at = -1
        self._cache_indices = {}

    def __len__(self) -> int:
        return len(self._records)

    def _enforce_capacity(self) -> None:
        if self.capacity >= 0:
            return

        while self.capacity < 0:
            self._records.pop()
            self._counter += 1
            self.capacity += 1

    @property
    def cache_indices(self) -> Mapping[NodeID, FrozenSet[NodeID]]:
        if self._last_cached_at < self._counter:
            records_by_node_id = groupby(0, self._records)
            self._cache_indices = {
                node_id: frozenset(record.node_id for record in location_records)
                for node_id, location_records in records_by_node_id.items()
            }
            self._last_cached_at = self._counter
        return self._cache_indices

    @to_tuple
    def _get_records(self, content_id: NodeID) -> Iterator[Location]:
        for location in self._records:
            if location.content_id == content_id:
                yield location

    def _touch_record(self, location: Location) -> None:
        if location == self._records[0]:
            return
        self._records.remove(location)
        self._records.appendleft(location)
        self._counter += 1

    def get_index(self, content_id: NodeID) -> FrozenSet[NodeID]:
        index = self.cache_indices[content_id]
        for location in self._get_records(content_id):
            self._touch_record(location)
        return index

    def add(self, location: Location) -> None:
        if location in self._records:
            self._touch_record(location)
        else:
            self._records.appendleft(location)
            self._counter += 1
            self.capacity -= 1
        self._enforce_capacity()

    def remove(self, location: Location) -> None:
        if location in self._records:
            self._records.remove(location)
            self._counter += 1
            self.capacity += 1
        else:
            raise KeyError(location)


class ContentManager(ContentManagerAPI):
    """
    Wraps separate datastores for Durable/Ephemeral/Cache
        - Durable: externally controlled, read-only
        - Ephemeral: populated purely from data found on network
            - eviction based on proximity metric
        - Cache: recently seen data
            - eviction based on LRU
    3. Expose single API for accessing data and location index
    """
    logger = logging.getLogger('alexandria.content_manager.ContentManager')

    def __init__(self,
                 center_id: NodeID,
                 durable_db: DurableDatabaseAPI,
                 config: StorageConfig = None,
                 ) -> None:
        if config is None:
            config = StorageConfig()
        self.config = config
        self.center_id = center_id

        # A database not subject to storage limits that will not have data
        # discarded or added to it.
        self.durable_db = durable_db
        self.rebuild_durable_index()

        # A database that will be dynamically populated by data we learn about
        # over the network.  Total size is capped.  Eviction of keys is based
        # on the kademlia distance metric, preferring keys that are near our
        # center.
        self.ephemeral_db = EphemeralDB(
            center_id=self.center_id,
            capacity=self.config.ephemeral_storage_size,
        )
        self.ephemeral_index = EphemeralIndex(
            center_id=self.center_id,
            capacity=self.config.ephemeral_index_size,
        )

        # A database that holds recently seen content and evicts based on an
        # LRU policy.
        self.cache_db = CacheDB(capacity=self.config.cache_storage_size)
        self.cache_index = CacheIndex(capacity=self.config.cache_index_size)

    def rebuild_durable_index(self) -> None:
        self.durable_index = {
            content_key_to_node_id(key): frozenset({self.center_id})
            for key in self.durable_db.keys()
        }

    def iter_content_keys(self) -> Tuple[bytes, ...]:
        return tuple(itertools.chain(
            self.durable_db.keys(),
            self.ephemeral_db.keys(),
            self.cache_db.keys(),
        ))

    def get_stats(self) -> ContentStats:
        return ContentStats(
            durable_item_count=len(self.durable_db),
            ephemeral_db_count=len(self.ephemeral_db),
            ephemeral_db_total_capacity=self.ephemeral_db.total_capacity,
            ephemeral_db_capacity=self.ephemeral_db.capacity,
            ephemeral_index_total_capacity=self.ephemeral_index.total_capacity,
            ephemeral_index_capacity=self.ephemeral_index.capacity,
            cache_db_count=len(self.cache_db),
            cache_db_total_capacity=self.cache_db.total_capacity,
            cache_db_capacity=self.cache_db.capacity,
            cache_index_total_capacity=self.cache_index.total_capacity,
            cache_index_capacity=self.cache_index.capacity,
        )

    def get_content(self, key: bytes) -> bytes:
        try:
            return self.durable_db.get(key)
        except KeyError:
            pass

        value_from_ephemeral: Optional[bytes]
        try:
            value_from_ephemeral = self.ephemeral_db.get(key)
        except KeyError:
            value_from_ephemeral = None

        value_from_cache: Optional[bytes]
        try:
            value_from_cache = self.cache_db.get(key)
        except KeyError:
            value_from_cache = None

        if value_from_cache is None and value_from_ephemeral is None:
            raise KeyError(key)
        elif value_from_cache is not None and value_from_ephemeral is not None:
            if value_from_cache != value_from_ephemeral:
                self.logger.warning(
                    "Data mismatch between ephemeral and cache data store for "
                    "key `%s`: ephemeral=%s  cache=%s",
                    encode_hex(key),
                    encode_hex(value_from_ephemeral),
                    encode_hex(value_from_cache),
                )
                self.cache_db.set(Content(key, value_from_ephemeral))
            return value_from_ephemeral
        elif value_from_ephemeral is not None:
            return value_from_ephemeral
        elif value_from_cache is not None:
            return value_from_cache
        else:
            raise Exception("Unreachable code block")

    def get_index(self, content_id: NodeID) -> FrozenSet[NodeID]:
        index_from_durable: FrozenSet[NodeID]
        try:
            index_from_durable = self.durable_index[content_id]
        except KeyError:
            index_from_durable = frozenset()

        index_from_ephemeral: FrozenSet[NodeID]
        try:
            index_from_ephemeral = self.ephemeral_index.get_index(content_id)
        except KeyError:
            index_from_ephemeral = frozenset()

        index_from_cache: FrozenSet[NodeID]
        try:
            index_from_cache = self.cache_index.get_index(content_id)
        except KeyError:
            index_from_cache = frozenset()

        return index_from_durable | index_from_ephemeral | index_from_cache

    def ingest_content(self, content: ContentBundle) -> None:
        if content.data is not None:
            self._ingest_content_data(content.key, content.data)
        content_id = content_key_to_node_id(content.key)
        self._ingest_content_index(content_id, content.node_id)

    def _ingest_content_data(self, key: bytes, data: bytes) -> None:
        if key in self.durable_db.keys():
            local_data = self.durable_db.get(key)
            if local_data != data:
                self.logger.warning(
                    "Encountered data mismatch for key %s: local=%s other=%s",
                    encode_hex(key),
                    encode_hex(local_data),
                    encode_hex(data),
                )
            # data which is in our durable db should not be placed in either
            # the ephemeral db or the cache db.  We assume that the value from
            # the durable DB is canonical
            return

        self.ephemeral_db.set(Content(key, data))
        self.cache_db.set(Content(key, data))

    def _ingest_content_index(self, content_id: NodeID, location_id: NodeID) -> None:
        location = Location(content_id, location_id)

        self.ephemeral_index.add(location)
        self.cache_index.add(location)
