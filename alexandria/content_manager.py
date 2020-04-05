import collections
import functools
import random
from typing import Collection, Deque, Dict, Iterator, Mapping, NamedTuple, Optional, Set, Tuple

from eth_utils import to_tuple
from eth_utils.toolz import groupby

from alexandria._utils import content_key_to_node_id
from alexandria.typing import NodeID
from alexandria.routing_table import compute_log_distance, compute_distance


class ContentBundle(NamedTuple):
    key: bytes
    data: Optional[bytes]
    node_id: NodeID


class Location(NamedTuple):
    key: bytes
    node_id: NodeID


class Record(NamedTuple):
    key: bytes
    data: bytes


class StorageConfig(NamedTuple):
    # number of bytes
    ephemeral_storage_size: int
    # number of index entries (fixed size per entry)
    ephemeral_index_size: int

    # number of bytes
    cache_storage_size: int
    # number of index entries (fixed size per entry)
    cache_index_size: int


MAX_DISTANCE = 2**256 - 1


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
    distance = compute_log_distance(content_id, center_id)
    return 1 - (1 / 2**distance)


def iter_furthest_keys(center_id: NodeID, keys: Collection[bytes]) -> Iterator[NodeID]:
    def sort_fn(key: bytes):
        content_id = content_key_to_node_id(key)
        return compute_distance(center_id, content_id)

    yield from sorted(keys, key=sort_fn, reverse=True)


class EphemeralDB:
    _db: Dict[bytes, bytes]

    def __init__(self, center_id: NodeID, capacity: int) -> None:
        self._db = {}
        self.center_id = center_id
        self.capacity = capacity

    @to_tuple
    def enforce_capacity(self) -> Iterator[bytes]:
        if self.capacity >= 0:
            return

        for key in iter_furthest_keys(self._db.keys(), self.center_id):
            content_id = content_key_to_node_id(key)
            evict_probability = get_eviction_probability(self.center_id, content_id)
            should_evict = random.random() < evict_probability
            if should_evict:
                yield key
                self.delete(key)

                if self.capacity >= 0:
                    break

    def get(self, key: bytes) -> None:
        return self._db[key]

    def set(self, key: bytes, data: bytes) -> None:
        try:
            if self.get(key) == data:
                return
            else:
                self.delete(key)
        except KeyError:
            pass

        self._db[key] = data
        self.capacity -= len(data)

    def delete(self, key: bytes) -> None:
        data = self._db.pop(key)
        self.capacity += len(data)


def iter_furthest_content_ids(center_id: NodeID,
                              content_ids: Collection[NodeID]) -> Iterator[NodeID]:
    sort_fn = functools.partial(compute_distance, center_id)

    yield from sorted(content_ids, key=sort_fn, reverse=True)


class EphemeralIndex:
    _index: Dict[NodeID, Set[NodeID]]

    def __init__(self, center_id: NodeID, capacity: int) -> None:
        self._index = {}
        self.center_id = center_id
        self.capacity = capacity

    @to_tuple
    def enforce_capacity(self) -> Iterator[Location]:
        if self.capacity >= 0:
            return

        for content_id in iter_furthest_content_ids(self.center_id, self._index.keys()):
            evict_probability = get_eviction_probability(self.center_id, content_id)
            should_evict = random.random() < evict_probability
            if should_evict:
                index = self._index.pop(key)
                for location_id in iter_furthest_content_ids(content_id, index):
                    index.remove(location_id)
                    yield Location(content_id, location_id)
                    self.capacity += 1

                    if self.capacity >= 0:
                        break

    def get(self, key: NodeID) -> Set[NodeID]:
        return self._index(key)

    def add(self, location: Location) -> None:
        key, location_id = location
        try:
            already_indexed = (location_id in self._index[key])
        except KeyError:
            self._index[key] = set()
            already_indexed = False

        if already_indexed:
            # already indexed so no change
            return
        else:
            self._index[key].add(location_id)
            self.capacity -= 1

    def delete(self, location: Location) -> None:
        key, location_id = location
        key_index = self._index[key]
        if location_id in key_index:
            key_index.remove(location_id)
            self.capacity += 1


class CacheDB:
    _db: Mapping[bytes, bytes]

    def __init__(self, capacity: int) -> None:
        self._db = {}
        self.capacity = capacity

    @to_tuple
    def enforce_capacity(self) -> Iterator[bytes]:
        ...

    def get(self, key: bytes) -> None:
        ...

    def set(self, key: bytes, data: bytes) -> None:
        ...

    def delete(self, key: bytes) -> None:
        ...


class ContentDB:
    """
    1. Maintain bounds on db and index sizes
    2. Maintain separate datastores for Durable/Ephemeral/Cache
        - Durable: externally controlled, read-only
        - Ephemeral: populated purely from data found on network
            - eviction based on proximity metric
        - Cache: recently seen data
            - eviction based on LRU
    3. Expose single API for accessing data and location index
    """
    durable_db: Mapping[bytes, bytes]
    durable_index: Mapping[NodeID, Set[NodeID]]

    ephemeral_index: Dict[NodeID, Set[NodeID]]
    _ephemeral_index_capacity: int

    _cache_db: Mapping[bytes, bytes]
    _cache_index: Mapping[NodeID, Set[NodeID]]

    cache_index_counter: int
    cache_db_counter: int

    center_id: NodeID

    def __init__(self,
                 center_id: NodeID,
                 durable_db: Mapping[bytes, bytes],
                 config: StorageConfig,
                 ) -> None:
        self.config = config
        self.center_id = center_id

        # A database not subject to storage limits that will not have data
        # discarded or added to it.
        self.durable_db = durable_db
        self.durable_index = collections.defaultdict(set)

        # A database that will be dynamically populated by data we learn about
        # over the network.  Total size is capped.  Eviction of keys is based
        # on the kademlia distance metric, preferring keys that are near our
        # center.
        self.ephemeral_db = EphemeralDB(
            center_id=self.center_id,
            capacity=self.config.ephemeral_storage_size,
        )
        self.ephemeral_index = collections.defaultdict(set)
        self._ephemeral_index_capacity = config.ephemeral_index_size

        self._ephemeral_db_capacity = config.ephemeral_storage_size
        self._cache_db_capacity = config.cache_storage_size

        # A database that holds recently seen content and evicts based on an
        # LRU policy.
        self.cache_db_records: Deque[Record] = collections.deque()
        self.cache_db_counter = 0
        self._cache_db = {}
        self._cache_db_capacity = config.cache_storage_size
        self._cache_db_at = -1

        self.cache_index_records: Deque[Location] = collections.deque()
        self.cache_index_counter = 0
        self._cache_index = collections.defaultdict(set)
        self._cache_index_capacity = config.cache_index_size
        self._cache_index_at = -1

    @property
    def cache_db(self) -> Mapping[bytes, bytes]:
        if self._cache_db_at < self.cache_db_counter:
            self._cache_db = {
                record.key: record.data
                for record in self.cache_db_records
            }
            self._cache_db_at = self.cache_db_counter
        return self._cache_db

    @property
    def cache_index(self) -> Mapping[bytes, Set[NodeID]]:
        if self._cache_index_at < self.cache_index_counter:
            records_by_node_id = groupby(1, self.cache_index_records)
            self._cache_index = {
                key: {record.node_id for record in location_records}
                for key, location_records in records_by_node_id.items()
            }
            self._cache_index_at = self.cache_index_counter
        return self._cache_index

    def process_content(self, content: ContentBundle) -> None:
        """
        """
        if content.data is not None:
            self.process_content_data(content.key, content.data)
        self.process_content_index(content.key, content.node_id)

    @to_tuple
    def _ensure_ephemeral_db_within_capacity(self) -> Tuple[bytes, ...]:
        """
        Enforces eviction policy for ephemeral database when over capacity.
        """

    def process_content_data(self, key: bytes, data: bytes) -> None:
        if key in self.durable_db:
            # data which is in our durable db should not be placed in either
            # the ephemeral db or the cache db.
            return

        if key not in self.ephemeral_db:
            self.insert_ephemeral_content(key, data)
            self._ephemeral_db_capacity -= len(data)
            self._ensure_ephemeral_db_within_capacity()

        cache_record = Record(key, data)
        if cache_record in self.cache_db_records:
            self.cache_db_records.remove(cache_record)
            self.cache_db_records.appendleft(cache_record)
            self.cache_db_counter += 1

    def process_content_index(self, key: bytes, node: NodeID) -> None:
        ...
