import enum
import hashlib
from typing import Optional

from eth_keys import keys

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
                 ) -> None:
        self._is_initiator = is_initiator

        self._private_key = private_key
        self._remote_node_id = remote_node_id
        self._status = SessionStatus.BEFORE

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
    async def handle_inbound_packet(self, packet: PacketAPI) -> Optional[PacketAPI]:
        if self.is_handshake_complete:
            raise NotImplementedError
        elif self.is_before_handshake:
            raise NotImplementedError
        elif self.is_during_handshake:
            if not isinstance(packet, HandshakePacket):
                raise Exception(f"Unhandled packet type: {type(packet)}")
            self._status = SessionStatus.AFTER
            return CompleteHandshakePacket(
                tag=compute_tag(self.local_node_id, self.remote_node_id),
            )
        else:
            raise NotImplementedError

    async def handle_outbound_packet(self, packet: PacketAPI) -> PacketAPI:
        if self.is_handshake_complete:
            raise NotImplementedError
        elif self.is_before_handshake:
            self._status = SessionStatus.DURING
            return packet

        elif self.is_during_handshake:
            raise NotImplementedError
        else:
            raise NotImplementedError

    def send_handshake_initiation(self) -> None:
        raise NotImplementedError

    def receive_handshake_response(self, packet: HandshakePacket) -> None:
        raise NotImplementedError

    def send_handshake_completion(self) -> None:
        raise NotImplementedError


#
# Receipient
#
class SessionRecipient(BaseSession):
    async def handle_inbound_packet(self, packet: PacketAPI) -> Optional[PacketAPI]:
        if self.is_handshake_complete:
            raise NotImplementedError
        elif self.is_before_handshake:
            self._status = SessionStatus.DURING
            return HandshakePacket(
                tag=compute_tag(self.local_node_id, self.remote_node_id),
            )
        elif self.is_during_handshake:
            if not isinstance(packet, CompleteHandshakePacket):
                raise Exception(f"Packet type not handled: {type(packet)}")
            self._status = SessionStatus.AFTER
            # TODO: session keys
            # TODO: more stuff....?
            raise NotImplementedError
        else:
            raise NotImplementedError

    async def handle_outbound_packet(self, packet: PacketAPI) -> PacketAPI:
        if self.is_handshake_complete:
            raise NotImplementedError
        elif self.is_before_handshake:
            raise NotImplementedError
        elif self.is_during_handshake:
            raise NotImplementedError
        else:
            raise NotImplementedError

    def receive_handshake_initiation(self, packet: MessagePacket) -> None:
        raise NotImplementedError

    def send_handshake_response(self) -> None:
        raise NotImplementedError

    def receive_handshake_completion(self, packet: CompleteHandshakePacket) -> None:
        raise NotImplementedError
