import enum
import hashlib

from eth_keys import keys
import trio

from alexandria.abc import PacketAPI, SessionAPI
from alexandria.packets import MessagePacket, HandshakePacket, CompleteHandshakePacket
from alexandria.tags import compute_tag
from alexandria.typing import NodeID, Tag


class SessionStatus(enum.Enum):
    BEFORE = enum.auto()
    DURING = enum.auto()
    AFTER = enum.auto()


class BaseSession(SessionAPI):
    def __init__(self,
                 is_initiator: bool,
                 private_key: keys.PrivateKey,
                 remote_node_id: NodeID,
                 outbound_packet_send_channel: trio.abc.SendChannel[PacketAPI],
                 ) -> None:
        self._is_initiator = is_initiator

        self._private_key = private_key
        self._remote_node_id = remote_node_id
        self._status = SessionStatus.BEFORE
        # TODO
        self._outbound_packet_send_channel = outbound_packet_send_channel

    @property
    def is_before_handshake(self) -> bool:
        return self._status is SessionStatus.BEFORE

    @property
    def is_handshake_complete(self) -> bool:
        return self._status is SessionStatus.AFTER

    @property
    def is_during_handshake(self) -> bool:
        return self._status is SessionStatus.DURING

    @property
    def is_initiator(self) -> bool:
        return self._is_initiator

    @property
    def private_key(self) -> keys.PrivateKey:
        return self._private_key

    @property
    def local_node_id(self) -> NodeID:
        return int.from_bytes(
            hashlib.sha256(self.private_key.public_key.to_bytes()).digest(),
            'big',
        )

    @property
    def remote_node_id(self) -> NodeID:
        return self._remote_node_id

    @property
    def tag(self) -> Tag:
        return compute_tag(
            source_node_id=self.local_node_id,
            destination_node_id=self.remote_node_id,
        )


"""
|   |  INITIATOR                     |    RECEIVER
----- ----------------------------------------------
| 1 | send_handshake_initiation()   --->  receive_handshake_initiation()
|   |                                             |
|   |                                             v
| 2 | receive_handshake_response()  <---  send_handshake_response()
|   |         |
|   |         v
| 3 | send_handshake_completion()   --->  receive_handshake_completion()
|   |         |                                   |
|   |         v                                   v
| 4 | HANDSHAKE_RESULT                    HANDSHAKE_RESULT
"""


#
# Initiator
#
class SessionInitiator(BaseSession):
    async def handle_inbound_packet(self, packet: PacketAPI) -> None:
        if self.is_handshake_complete:
            raise NotImplementedError
        elif self.is_before_handshake:
            raise NotImplementedError
        elif self.is_during_handshake:
            await self.receive_handshake_response(packet)
            await self.send_handshake_completion()
        else:
            raise Exception("Invalid state")

    async def handle_outbound_packet(self, packet: PacketAPI) -> None:
        if self.is_handshake_complete:
            raise NotImplementedError
        elif self.is_before_handshake:
            await self.send_handshake_initiation(packet)
        elif self.is_during_handshake:
            raise NotImplementedError
        else:
            raise Exception("Invalid state")

    async def send_handshake_initiation(self, packet: PacketAPI) -> None:
        self._status = SessionStatus.DURING
        await self._outbound_packet_send_channel.send(packet)

    async def receive_handshake_response(self, packet: HandshakePacket) -> None:
        if not isinstance(packet, HandshakePacket):
            raise Exception(f"Unhandled packet type: {type(packet)}")
        self._status = SessionStatus.AFTER

    async def send_handshake_completion(self) -> None:
        await self._outbound_packet_send_channel.send(CompleteHandshakePacket(
            tag=compute_tag(self.local_node_id, self.remote_node_id),
        ))


#
# Receipient
#
class SessionRecipient(BaseSession):
    async def handle_inbound_packet(self, packet: PacketAPI) -> None:
        if self.is_handshake_complete:
            raise NotImplementedError
        elif self.is_before_handshake:
            await self.receive_handshake_initiation(packet)
            await self.send_handshake_response()
        elif self.is_during_handshake:
            await self.receive_handshake_completion(packet)
        else:
            raise Exception("Invalid state")

    async def handle_outbound_packet(self, packet: PacketAPI) -> None:
        if self.is_handshake_complete:
            raise NotImplementedError
        elif self.is_before_handshake:
            raise NotImplementedError
        elif self.is_during_handshake:
            raise NotImplementedError
        else:
            raise Exception("Invalid state")

    async def receive_handshake_initiation(self, packet: MessagePacket) -> None:
        self._status = SessionStatus.DURING

    async def send_handshake_response(self) -> None:
        await self._outbound_packet_send_channel.send(HandshakePacket(
            tag=compute_tag(self.local_node_id, self.remote_node_id),
        ))

    async def receive_handshake_completion(self, packet: CompleteHandshakePacket) -> None:
        if not isinstance(packet, CompleteHandshakePacket):
            raise Exception(f"Packet type not handled: {type(packet)}")
        self._status = SessionStatus.AFTER
        # TODO: session keys
