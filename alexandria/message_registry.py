from typing import Any, Dict, Type

import ssz
from ssz import sedes

from alexandria.abc import RegistryAPI, TPayload


class Registry(RegistryAPI):
    _payload_sedes: Dict[int, Type[Any]]
    _message_id_lookup: Dict[Type[Any], int]

    def __init__(self) -> None:
        self._payload_sedes = {}
        self._message_id_lookup = {}

    def register(self, message_id: int, payload_type: Type[Any]) -> None:
        if message_id in self._payload_sedes:
            raise ValueError(
                f"Cannot register {payload_type} with "
                f"`message_id={message_id}`. The message id is already "
                f"registered for {self._payload_sedes[message_id]}"
            )
        elif payload_type in self._message_id_lookup:
            raise ValueError(
                f"Cannot register {payload_type} with "
                f"`message_id={message_id}`. The payload type is already "
                f"registered under "
                f"`message_id={self._message_id_lookup[payload_type]}`"
            )

        self._payload_sedes[message_id] = payload_type
        self._message_id_lookup[payload_type] = message_id

    def get_sedes(self, message_id: int) -> sedes.Serializable:
        return self._payload_sedes[message_id]

    def get_message_id(self, payload_type: Type[TPayload]) -> int:
        return self._message_id_lookup[payload_type]

    def decode_payload(self, data: bytes) -> sedes.Serializable:
        message_id = data[0]
        try:
            sedes = self._payload_sedes[message_id]
        except KeyError:
            raise ValueError(f"Unknown message type: {message_id}")
        return ssz.decode(data[1:], sedes)


default_registry = Registry()
register = default_registry.register
