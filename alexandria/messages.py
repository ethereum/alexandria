import ssz

from alexandria.abc import MessageAPI, Node, RegistryAPI, TPayload
from alexandria.constants import ALL_BYTES
from alexandria.message_registry import default_registry
from alexandria.payloads import (
    Ping, Pong,
    FindNodes, FoundNodes,
    Advertise, Ack,
    Locate, Locations,
    Retrieve, Chunk,
)


class Message(MessageAPI[TPayload]):
    def __init__(self,
                 payload: TPayload,
                 node: Node,
                 *,
                 message_registry: RegistryAPI = default_registry,
                 ) -> None:
        self.payload = payload
        self.node = node
        self.message_id = message_registry.get_message_id(type(payload))

    def __str__(self) -> str:
        return f"Message[{self.payload} -> {self.node}]"

    def to_bytes(self) -> None:
        return ALL_BYTES[self.message_id] + ssz.encode(self.payload)


#
# Base DHT Messages
#
default_registry.register(0, Ping)
default_registry.register(1, Pong)
default_registry.register(2, FindNodes)
default_registry.register(3, FoundNodes)


#
# Content Management Messages
#
default_registry.register(4, Advertise)
default_registry.register(5, Ack)
default_registry.register(6, Locate)
default_registry.register(7, Locations)
default_registry.register(8, Retrieve)
default_registry.register(9, Chunk)
