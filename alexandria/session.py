import enum
import hashlib
import secrets

from eth_keys import keys
from eth_utils import ValidationError
import trio

from alexandria.abc import MessageAPI, PacketAPI, SessionAPI
from alexandria.exceptions import HandshakeFailure, DecryptionError
from alexandria.handshake import compute_session_keys, create_id_nonce_signature, SessionKeys
from alexandria.packets import (
    MessagePacket,
    HandshakeResponse,
    CompleteHandshakePacket,
    get_random_auth_tag,
    get_random_encrypted_data,
    get_random_id_nonce,
    compute_handshake_response_magic,
)
from alexandria.tags import compute_tag, recover_source_id_from_tag
from alexandria.typing import NodeID, Tag, AES128Key, IDNonce


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
        self._outbound_message_buffer_channels = trio.open_memory_channel[MessageAPI](256)
        self._inbound_packet_buffer_channels = trio.open_memory_channel[PacketAPI](256)
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
    #
    # Message API
    #
    async def handle_outbound_message(self, message: MessageAPI) -> None:
        if self.is_handshake_complete:
            # TODO: construct MessagePacket
            raise NotImplementedError
        elif self.is_before_handshake:
            self._initial_message = message
            await self.send_handshake_initiation()
        elif self.is_during_handshake:
            self.logger.debug(
                'handshake in progress: placing outbound message in queue: %s',
                message,
            )
            # TODO: handle full buffer...
            self._outbound_message_buffer_channels[0].send_nowait(message)
        else:
            raise Exception("Invalid state")

    #
    # Packet API
    #
    async def handle_inbound_packet(self, packet: PacketAPI) -> None:
        if self.is_handshake_complete:
            raise NotImplementedError
        elif self.is_before_handshake:
            raise NotImplementedError
        elif self.is_during_handshake:
            if isinstance(packet, HandshakeResponse):
                self._session_keys = await self.receive_handshake_response(packet)
                self._status = SessionStatus.AFTER
                await self.send_handshake_completion(self._session_keys)
            else:
                self._inbound_packet_buffer_channels[0].send_nowait(packet)
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

    async def send_handshake_initiation(self) -> None:
        self._status = SessionStatus.DURING
        self._initiating_packet = MessagePacket(
            tag=self.tag,
            auth_tag=get_random_auth_tag(),
            encrypted_message=get_random_encrypted_data(),
        )
        await self._outbound_packet_send_channel.send(self._initiating_packet)

    async def receive_handshake_response(self, packet: HandshakeResponse) -> SessionKeys:
        if not isinstance(packet, HandshakeResponse):
            raise Exception(f"Unhandled packet type: {type(packet)}")
        if not secrets.compare_digest(packet.token, self._initiating_packet.auth_tag):
            raise ValidationError("Mismatch between token")

        # compute session keys
        ephemeral_private_key = keys.PrivateKey(secrets.token_bytes(32))

        remote_public_key_object = packet.public_key
        remote_public_key_uncompressed = remote_public_key_object.to_bytes()
        session_keys = compute_session_keys(
            local_private_key=ephemeral_private_key,
            remote_public_key=remote_public_key_uncompressed,
            local_node_id=self.local_enr.node_id,
            remote_node_id=self.remote_node_id,
            id_nonce=packet.id_nonce,
            is_locally_initiated=True,
        )
        return session_keys

    async def send_handshake_completion(self, session_keys) -> None:
        # prepare response packet
        id_nonce_signature = create_id_nonce_signature(
            id_nonce=session_keys.id_nonce,
            ephemeral_public_key=session_keys.private_key.public_key,
            private_key=self.local_private_key,
        )

        auth_header_packet = MessagePacket.prepare(
            tag=self.tag,
            auth_tag=get_random_auth_tag(),
            id_nonce=session_keys.id_nonce,
            message=self.initial_message,
            initiator_key=session_keys.encryption_key,
            id_nonce_signature=id_nonce_signature,
            auth_response_key=session_keys.auth_response_key,
            ephemeral_public_key=session_keys.private_key.public_key,
            public_key=self.local_private_key.public_key,
        )
        await self._outbound_packet_send_channel.send(auth_header_packet)


#
# Receipient
#
class SessionRecipient(BaseSession):
    #
    # Message API
    #
    async def handle_outbound_message(self, message: MessageAPI) -> None:
        if self.is_handshake_complete:
            raise NotImplementedError
        elif self.is_before_handshake:
            raise NotImplementedError
        elif self.is_during_handshake:
            raise NotImplementedError
        else:
            raise Exception("Invalid state")

    #
    # Packet API
    #
    async def handle_inbound_packet(self, packet: PacketAPI) -> None:
        if self.is_handshake_complete:
            raise NotImplementedError
        elif self.is_before_handshake:
            if isinstance(packet, MessagePacket):
                await self.receive_handshake_initiation(packet)
                await self.send_handshake_response(packet)
            else:
                # TODO: full buffer handling
                self._inbound_packet_buffer_channels.send_nowait(packet)
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

    async def send_handshake_response(self, initiation_packet: MessagePacket) -> None:
        magic = compute_handshake_response_magic(self.remote_node_id)
        response_packet = HandshakeResponse(
            tag=self.tag,
            magic=magic,
            token=initiation_packet.auth_tag,
            id_nonce=get_random_id_nonce(),
            public_key=self.private_key.public_key,
        )
        await self._outbound_packet_send_channel.send(response_packet)

    async def receive_handshake_completion(self, packet: CompleteHandshakePacket) -> None:
        remote_node_id = recover_source_id_from_tag(
            packet.tag,
            self.local_node_id,
        )
        if remote_node_id != self.remote_node_id:
            raise ValidationError(
                f"Remote node ids do not match: {remote_node_id} != {self.remote_node_id}"
            )

        ephemeral_public_key = packet.header.ephemeral_public_key
        try:
            keys.PublicKey(ephemeral_public_key)
        except Exception as err:
            raise ValidationError('Invalid remote public key') from err

        session_keys = compute_session_keys(
            local_private_key=self.local_private_key,
            remote_public_key=ephemeral_public_key,
            local_node_id=self.local_enr.node_id,
            remote_node_id=self.remote_node_id,
            id_nonce=self.who_are_you_packet.id_nonce,
            is_locally_initiated=False,
        )

        self.decrypt_and_validate_auth_response(
            packet,
            session_keys.auth_response_key,
            self.who_are_you_packet.id_nonce,
        )
        # TODO: where should this live
        # message = self.decrypt_and_validate_message(
        #     packet,
        #     session_keys.decryption_key,
        # )
        return session_keys, packet

    def decrypt_and_validate_auth_response(self,
                                           auth_header_packet: CompleteHandshakePacket,
                                           auth_response_key: AES128Key,
                                           id_nonce: IDNonce,
                                           ) -> None:
        try:
            id_nonce_signature = auth_header_packet.decrypt_auth_response(auth_response_key)
        except DecryptionError as error:
            raise HandshakeFailure("Unable to decrypt auth response") from error
        except ValidationError as error:
            raise HandshakeFailure("Invalid auth response content") from error

        try:
            self.identity_scheme.validate_id_nonce_signature(
                signature=id_nonce_signature,
                id_nonce=id_nonce,
                ephemeral_public_key=auth_header_packet.header.ephemeral_public_key,
                public_key=auth_header_packet.header.public_key,
            )
        except ValidationError as error:
            raise HandshakeFailure("Invalid id nonce signature in auth response") from error

        return enr
