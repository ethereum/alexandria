import collections
import functools
import itertools
import logging
import random
from typing import (
    Collection,
    Deque,
    Dict,
    FrozenSet,
    Iterator,
    KeysView,
    Mapping,
    Optional,
    Set,
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
from alexandria.routing_table import compute_log_distance, compute_distance


def get_eviction_probability(content_id: NodeID, center_id: NodeID) -> float:
    """
    log-distance:
     - 0: 0%
     - 1: 50%
     - 2: 75%
     - ...
     - 256: 99%
    """
    if content_id == center_id:
        return 0.0
    distance: int
    distance = compute_log_distance(content_id, center_id)
    return float(1 - (1 / 2**distance))


def iter_furthest_keys(center_id: NodeID, keys: Collection[bytes]) -> Iterator[bytes]:
    def sort_fn(key: bytes) -> int:
        content_id = content_key_to_node_id(key)
        return compute_distance(center_id, content_id)

    yield from sorted(keys, key=sort_fn, reverse=True)


class BaseContentDB(ContentDatabaseAPI):
    @property
    def has_capacity(self) -> bool:
        return self.capacity > 0


class EphemeralDB(BaseContentDB):
    _db: Dict[bytes, bytes]

    def __init__(self, center_id: NodeID, capacity: int) -> None:
        self._db = {}
        self.center_id = center_id
        self.capacity = capacity
        self.total_capacity = capacity

    def __len__(self) -> int:
        return len(self._db)

    def keys(self) -> KeysView[bytes]:
        return collections.KeysView(self._db)

    def _enforce_capacity(self) -> None:
        if self.capacity >= 0:
            return

        for key in iter_furthest_keys(self.center_id, self._db.keys()):
            content_id = content_key_to_node_id(key)
            evict_probability = get_eviction_probability(self.center_id, content_id)
            should_evict = random.random() < evict_probability
            if should_evict:
                self.delete(key)

                if self.capacity >= 0:
                    break

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
        self.capacity -= len(data)
        self._enforce_capacity()

    def delete(self, key: bytes) -> None:
        data = self._db.pop(key)
        self.capacity += len(data)


def iter_furthest_content_ids(center_id: NodeID,
                              content_ids: Collection[NodeID]) -> Iterator[NodeID]:
    sort_fn = functools.partial(compute_distance, center_id)

    yield from sorted(content_ids, key=sort_fn, reverse=True)


class EphemeralIndex(ContentIndexAPI):
    _indices: Dict[NodeID, Set[NodeID]]

    def __init__(self, center_id: NodeID, capacity: int) -> None:
        self._indices = {}
        self.center_id = center_id
        self.capacity = capacity
        self.total_capacity = capacity

    def __len__(self) -> int:
        return sum(len(index) for index in self._indices.values())

    def _enforce_capacity(self) -> None:
        if self.capacity >= 0:
            return

        for content_id in iter_furthest_content_ids(self.center_id, self._indices.keys()):
            evict_probability = get_eviction_probability(self.center_id, content_id)
            should_evict = random.random() < evict_probability
            if should_evict:
                index = self._indices[content_id]
                for location_id in iter_furthest_content_ids(content_id, index):
                    index.remove(location_id)
                    self.capacity += 1

                    if len(index) == 0:
                        self._indices.pop(content_id)

                    if self.capacity >= 0:
                        break
                if self.capacity >= 0:
                    break

    def get_index(self, key: NodeID) -> FrozenSet[NodeID]:
        return frozenset(self._indices[key])

    def add(self, location: Location) -> None:
        key, location_id = location
        try:
            already_indexed = (location_id in self._indices[key])
        except KeyError:
            self._indices[key] = set()
            already_indexed = False

        if already_indexed:
            # already indexed so no change
            return
        else:
            self._indices[key].add(location_id)
            self.capacity -= 1
            self._enforce_capacity()

    def remove(self, location: Location) -> None:
        key, location_id = location
        index = self._indices[key]
        if location_id in index:
            index.remove(location_id)
            self.capacity += 1


class CacheDB(BaseContentDB):
    _records: Deque[Content]
    _cache_db: Mapping[bytes, bytes]
    _counter: int

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.total_capacity = capacity
        self._records = collections.deque()
        self._counter = 0
        self._last_cached_at = -1
        self._cache_db = {}

    def __len__(self) -> int:
        return len(self._records)

    def keys(self) -> KeysView[bytes]:
        return collections.KeysView(self.cache_db)

    @property
    def cache_db(self) -> Mapping[bytes, bytes]:
        if self._last_cached_at < self._counter:
            self._cache_db = {
                content.key: content.data
                for content in self._records
            }
            self._last_cached_at = self._counter
        return self._cache_db

    def _enforce_capacity(self) -> None:
        while self.capacity < 0:
            oldest_content = self._records.pop()
            self._counter += 1
            self.capacity += len(oldest_content.data)

    def _touch_record(self, content: Content) -> None:
        if content == self._records[0]:
            return
        self._records.remove(content)
        self._records.appendleft(content)
        self._counter += 1

    def _get_record(self, key: bytes) -> Content:
        for content in self._records:
            if content.key == key:
                return content
        else:
            raise KeyError(key)

    def get(self, key: bytes) -> bytes:
        content = self._get_record(key)
        self._touch_record(content)
        return content.data

    def set(self, content: Content) -> None:
        # re-setting the same value
        if content in self._records:
            self._touch_record(content)
            return

        # if db already has key, clear out existing value
        try:
            previous_content = self._get_record(content.key)
        except KeyError:
            pass
        else:
            self.capacity += len(previous_content.data)
            self._records.remove(previous_content)

        # set new record
        self._records.appendleft(content)
        self._counter += 1
        self.capacity -= len(content.data)
        self._enforce_capacity()

    def delete(self, key: bytes) -> None:
        content = self._get_record(key)
        self._records.remove(content)
        self._counter += 1
        self.capacity += len(content.data)


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
