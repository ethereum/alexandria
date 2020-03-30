from typing import Type

from alexandria.abc import PacketAPI, TPacket
from alexandria.constants import ALL_BYTES
from alexandria.typing import Tag


class MessagePacket(PacketAPI):
    packet_id = 0

    def __init__(self,
                 tag: Tag,
                 ) -> None:
        self.tag = tag

    def to_wire_bytes(self) -> bytes:
        return self.tag

    @classmethod
    def from_wire_bytes(cls: Type[TPacket], data: bytes) -> TPacket:
        return MessagePacket(data)


class HandshakePacket(PacketAPI):
    packet_id = 1

    def __init__(self,
                 tag: Tag,
                 ) -> None:
        self.tag = tag

    def to_wire_bytes(self) -> bytes:
        return self.tag

    @classmethod
    def from_wire_bytes(cls: Type[TPacket], data: bytes) -> TPacket:
        return HandshakePacket(data)


class CompleteHandshakePacket(PacketAPI):
    packet_id = 2

    def __init__(self,
                 tag: Tag,
                 ) -> None:
        self.tag = tag

    def to_wire_bytes(self) -> bytes:
        return self.tag

    @classmethod
    def from_wire_bytes(cls: Type[TPacket], data: bytes) -> TPacket:
        return CompleteHandshakePacket(data)


def encode_packet(packet: PacketAPI):
    return ALL_BYTES[packet.packet_id] + packet.to_wire_bytes()


def decode_packet(data: bytes) -> PacketAPI:
    packet_id = data[0]
    if packet_id == 0:
        return MessagePacket.from_wire_bytes(data[1:])
    elif packet_id == 1:
        return HandshakePacket.from_wire_bytes(data[1:])
    elif packet_id == 2:
        return CompleteHandshakePacket.from_wire_bytes(data[1:])
    else:
        raise TypeError(f"Unknown packet id: {packet_id}")
