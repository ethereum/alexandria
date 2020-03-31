from typing import Callable, Mapping, Type

import ssz
from ssz import sedes

from alexandria.abc import Endpoint, MessageAPI, RegistryAPI, TPayload
from alexandria.constants import ALL_BYTES
from alexandria.typing import NodeID


class Registry(RegistryAPI):
    _payload_sedes: Mapping[int, Type[ssz.Serializable]]
    _message_id_lookup: Mapping[Type[ssz.Serializable], int]

    def __init__(self) -> None:
        self._payload_sedes = {}
        self._message_id_lookup = {}

    def register(self, message_id: int) -> Callable[[Type[TPayload]], Type[TPayload]]:
        def outer(payload_type: Type[TPayload]) -> Type[TPayload]:
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
            return payload_type
        return outer

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


class Message(MessageAPI[TPayload]):
    def __init__(self,
                 payload: TPayload,
                 node_id: NodeID,
                 endpoint: Endpoint,
                 *,
                 message_registry: RegistryAPI = default_registry,
                 ) -> None:
        self.payload = payload
        self.node_id = node_id
        self.endpoint = endpoint
        self.message_id = message_registry.get_message_id(type(payload))

    def __str__(self) -> str:
        return f"Message[{self.message_id}/{self.payload}]"

    def to_bytes(self) -> None:
        return ALL_BYTES[self.message_id] + ssz.encode(self.payload)


@register(message_id=0)
class Ping(sedes.Serializable):
    fields = (
        ('id', sedes.uint16),
    )

    id: int


@register(message_id=1)
class Pong(sedes.Serializable):
    fields = (
        ('ping_id', sedes.uint16),
    )

    ping_id: int
