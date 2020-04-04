from ssz import sedes


class ByteList(sedes.List):  # type: ignore
    def __init__(self, max_length: int) -> None:
        super().__init__(element_sedes=sedes.uint, max_length=max_length)

    def serialize(self, value: bytes) -> bytes:
        return value

    def deserialize(self, value: bytes) -> bytes:
        return value


byte_list = ByteList(2**32)
