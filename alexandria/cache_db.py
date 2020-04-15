import collections
from typing import KeysView, Sized, TypeVar

from alexandria.abc import LimitedCapacityDatabaseAPI, TKey


TValue = TypeVar('TValue', bound=Sized)


class CacheDB(LimitedCapacityDatabaseAPI[TKey, TValue]):
    _records: 'collections.OrderedDict[TKey, TValue]'

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.total_capacity = capacity
        self._records = collections.OrderedDict()

    def __len__(self) -> int:
        return len(self._records)

    @property
    def has_capacity(self) -> bool:
        return self.capacity > 0

    def keys(self) -> KeysView[TKey]:
        return self._records.keys()

    def _enforce_capacity(self) -> None:
        while self.capacity < 0:
            _, value = self._records.popitem(last=False)
            self.capacity += len(value)

    def has(self, key: TKey) -> bool:
        return key in self._records

    def get(self, key: TKey) -> TValue:
        self._records.move_to_end(key, last=False)
        return self._records[key]

    def set(self, key: TKey, value: TValue) -> None:
        try:
            current_data = self._records.pop(key)
        except KeyError:
            pass
        else:
            self.capacity += len(current_data)

        self._records[key] = value
        self.capacity -= len(value)
        self._enforce_capacity()

    def delete(self, key: TKey) -> None:
        value = self._records.pop(key)
        self.capacity += len(value)
