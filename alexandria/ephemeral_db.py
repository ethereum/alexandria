import bisect
import functools
from typing import Any, Callable, Dict, KeysView, List, Generic, Sized, TypeVar

from alexandria.abc import LimitedCapacityDatabaseAPI, TKey


TValue = TypeVar('TValue', bound=Sized)


@functools.total_ordering
class SortableKey(Generic[TKey]):
    key: TKey
    distance: int

    def __init__(self, key: TKey, distance: int) -> None:
        self.key = key
        self.distance = distance

    def __eq__(self, other: Any) -> bool:
        if type(other) is type(self):
            return self.key == other.key  # type: ignore
        raise TypeError("Invalid comparison")

    def __lt__(self, other: Any) -> bool:
        if type(other) is type(self):
            return self.distance < other.distance  # type: ignore
        raise TypeError("Invalid comparison")


class EphemeralDB(LimitedCapacityDatabaseAPI[TKey, TValue]):
    _db: Dict[TKey, TValue]
    _keys_by_distance: List[SortableKey[TKey]]

    def __init__(self,
                 capacity: int,
                 distance_fn: Callable[[TKey], int]) -> None:
        self._db = {}
        self._keys_by_distance = []
        self.capacity = capacity
        self.total_capacity = capacity
        self.distance_fn = distance_fn

    @property
    def has_capacity(self) -> bool:
        return self.capacity > 0

    def __len__(self) -> int:
        return len(self._db)

    def keys(self) -> KeysView[TKey]:
        return self._db.keys()

    def _enforce_capacity(self) -> None:
        while self.capacity < 0:
            most_distant_key = self._keys_by_distance.pop()
            most_distant_value = self._db.pop(most_distant_key.key)
            self.capacity += len(most_distant_value)

    def has(self, key: TKey) -> bool:
        return key in self._db

    def get(self, key: TKey) -> TValue:
        return self._db[key]

    def set(self, key: TKey, value: TValue) -> None:
        try:
            if self.get(key) == value:
                return
            else:
                self.delete(key)
        except KeyError:
            pass

        self._db[key] = value
        distance = self.distance_fn(key)
        bisect.insort_right(self._keys_by_distance, SortableKey(key, distance))
        self.capacity -= len(value)
        self._enforce_capacity()

    def delete(self, key: TKey) -> None:
        value = self._db.pop(key)
        self.capacity += len(value)
        distance = self.distance_fn(key)
        key_index = bisect.bisect_left(self._keys_by_distance, SortableKey(key, distance))
        if self._keys_by_distance[key_index].key == key:
            self._keys_by_distance.pop(key_index)
        else:
            raise Exception("Invariant")
