from typing import Any, Optional, Tuple

from eth_typing import Hash32

from ssz import sedes
from ssz.cache.utils import get_key
from ssz.typing import CacheObj, TDeserialized, TSerializable
from ssz.sedes.base import TSedes
from ssz.exceptions import DeserializationError
from ssz.utils import merkleize, merkleize_with_cache, pack


class ByteList(sedes.List):  # type: ignore
    def __init__(self, max_length: int) -> None:
        super().__init__(element_sedes=sedes.uint8, max_length=max_length)

    def serialize(self, value: bytes) -> bytes:
        return value

    def deserialize(self, value: bytes) -> bytes:
        return value


byte_list = ByteList(2**32)


class Maybe(sedes.BaseSedes[TSerializable, TDeserialized]):
    is_fixed_sized = False

    def __init__(self, element_sedes: TSedes) -> None:
        self.element_sedes = element_sedes

    def __hash__(self) -> int:
        return hash((hash(type(self)), self.element_sedes))

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Maybe) and other.element_sedes == self.element_sedes

    def get_sedes_id(self) -> str:
        return "TODO"

    def get_fixed_size(self) -> int:
        raise NotImplementedError

    def serialize(self, value: Optional[TSerializable]) -> bytes:
        if value is None:
            return b'\x00'
        else:
            return b'\x01' + self.element_sedes.serialize(value)

    def deserialize(self, data: bytes) -> Optional[TDeserialized]:
        try:
            signal_byte = data[0]
        except IndexError:
            raise DeserializationError("Cannot deserialize empty byte string")

        if signal_byte == 0:
            return None
        elif signal_byte == 1:
            return self.element_sedes.deserialize(data[1:])
        else:
            raise DeserializationError(f"Invalid signal byte: {hex(signal_byte)}")

    #
    # Tree hashing
    #
    def get_hash_tree_root(self, value: TSerializable) -> Hash32:
        serialized_value = self.serialize(value)
        return merkleize(pack((serialized_value,)))

    def get_hash_tree_root_and_leaves(
        self, value: TSerializable, cache: CacheObj
    ) -> Tuple[Hash32, CacheObj]:
        serialized_value = self.serialize(value)
        return merkleize_with_cache(pack((serialized_value,)), cache=cache)

    def get_key(self, value: Any) -> str:
        return get_key(self, value)


class maybe(Maybe[Any, Any]):
    pass
