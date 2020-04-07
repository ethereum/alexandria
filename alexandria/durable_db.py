import collections
import logging
from pathlib import Path
from typing import (
    KeysView,
)

import plyvel

from alexandria.abc import DurableDatabaseAPI


class DurableDB(DurableDatabaseAPI):
    logger = logging.getLogger("alexandria.durable_db.DurableDB")

    # Creates db as a class variable to avoid level db lock error
    def __init__(self,
                 db_path: Path = None,
                 max_open_files: int = None) -> None:
        if not db_path:
            raise TypeError("Please specifiy a valid path for your database.")
        self.db_path = db_path
        self.db = plyvel.DB(
            str(db_path),
            create_if_missing=True,
            error_if_exists=False,
            max_open_files=max_open_files
        )

    def __len__(self) -> int:
        return len(tuple(self.keys()))

    def keys(self) -> KeysView[bytes]:
        return collections.KeysView(self.db.iterator(include_value=False))

    def get(self, key: bytes) -> bytes:
        data = self.db.get(key)
        if data is None:
            raise KeyError(key)
        return bytes(data)

    def set(self, key: bytes, data: bytes) -> None:
        self.db.put(key, data)

    def delete(self, key: bytes) -> None:
        if self.db.get(key) is None:
            raise KeyError(key)
        self.db.delete(key)
