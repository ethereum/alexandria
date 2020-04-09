import enum
import logging
import secrets
import time
from typing import Tuple
import uuid

from eth_keys import keys
from eth_utils import encode_hex, ValidationError
from ssz import sedes
import trio

from alexandria._utils import humanize_node_id, public_key_to_node_id
from alexandria.abc import (
    EventsAPI,
    MessageAPI,
    NetworkPacket,
    Node,
    PacketAPI,
    SessionAPI,
    SessionKeys,
)
from alexandria.exceptions import CorruptSession, HandshakeFailure, DecryptionError
from alexandria.handshake import (
    compute_session_keys,
    create_id_nonce_signature,
    create_id_nonce_signature_input,
)
from alexandria.messages import Message
from alexandria.packets import (
    CompleteHandshakePacket,
    compute_handshake_response_magic,
    get_random_auth_tag,
    get_random_encrypted_data,
    get_random_id_nonce,
    HandshakeResponse,
    MessagePacket,
)
from alexandria.tags import compute_tag, recover_source_id_from_tag
from alexandria.typing import NodeID, Tag, AES128Key, IDNonce


class SessionStatus(enum.Enum):
    BEFORE = '|'
    DURING = '~'
    AFTER = '-'


class BaseSession(SessionAPI):
    logger = logging.getLogger('alexandria.session.Session')

    _session_keys: SessionKeys

    def __init__(self,
                 private_key: keys.PrivateKey,
                 remote_node: Node,
                 events: EventsAPI,
                 outbound_packet_send_channel: trio.abc.SendChannel[NetworkPacket],
                 inbound_message_send_channel: trio.abc.SendChannel[MessageAPI[sedes.Serializable]],
                 ) -> None:
        self._private_key = private_key
        self.remote_node = remote_node
        self.remote_node_id = remote_node.node_id
        self.remote_endpoint = remote_node.endpoint
        self._status = SessionStatus.BEFORE

        self._events = events

        self._outbound_message_buffer_channels = trio.open_memory_channel[MessageAPI[sedes.Serializable]](256)  # noqa: E501
        self._inbound_packet_buffer_channels = trio.open_memory_channel[PacketAPI](256)

        # TODO
        self._outbound_packet_send_channel = outbound_packet_send_channel
        self._inbound_message_send_channel = inbound_message_send_channel

        self.last_message_at = 0.0

        self.session_id = uuid.uuid4()

    def __str__(self) -> str:
        if self.is_initiator:
            connector = f'-{self._status.value}->'
        else:
            connector = f'<-{self._status.value}-'

        return (
            "Session["
            f"{humanize_node_id(self.local_node_id)}"
            f"{connector}"
            f"{humanize_node_id(self.remote_node_id)}"
            f"@{self.remote_endpoint}"
            "]"
        )

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
    def private_key(self) -> keys.PrivateKey:
        return self._private_key

    @property
    def local_node_id(self) -> NodeID:
        return public_key_to_node_id(self.private_key.public_key)

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
    is_initiator = True

    #
    # Message API
    #
    async def handle_outbound_message(self, message: MessageAPI[sedes.Serializable]) -> None:
        if self.is_handshake_complete:
            self.logger.debug("%s: sending message: %s", self, message)
            packet = MessagePacket.prepare(
                tag=self.tag,
                auth_tag=get_random_auth_tag(),
                message=message,
                key=self._session_keys.encryption_key,
            )
            await self._outbound_packet_send_channel.send(NetworkPacket(
                packet=packet,
                endpoint=self.remote_endpoint,
            ))
        elif self.is_before_handshake:
            self.logger.debug(
                "%s: outbound message triggered handshake initiation: %s",
                self,
                message,
            )
            self._initial_message = message
            await self.send_handshake_initiation()
        elif self.is_during_handshake:
            self.logger.debug(
                '%s: handshake in progress: placing outbound message in queue: %s',
                self,
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
        self.last_message_at = time.monotonic()

        if self.is_handshake_complete:
            if isinstance(packet, MessagePacket):
                payload = packet.decrypt_payload(self._session_keys.decryption_key)
                message = Message(
                    payload=payload,
                    node=self.remote_node,
                )
                self.logger.debug(
                    '%s: processed inbound message packet: %s',
                    self,
                    message,
                )
                await self._inbound_message_send_channel.send(message)
            else:
                self.logger.debug(
                    '%s: Ignoring packet of type %s received after handshake complete',
                    self,
                    type(packet),
                )
        elif self.is_before_handshake:
            raise NotImplementedError
        elif self.is_during_handshake:
            if isinstance(packet, HandshakeResponse):
                self._session_keys, ephemeral_public_key = await self.receive_handshake_response(
                    packet
                )
                self._status = SessionStatus.AFTER
                await self._events.handshake_complete.trigger(self)

                await self.send_handshake_completion(
                    self._session_keys,
                    ephemeral_public_key,
                    packet,
                )
            else:
                raise CorruptSession(
                    f"Suspected corrupted session. Got {packet} packet during handshake initiation."
                )
        else:
            raise Exception("Invalid state")

    async def send_handshake_initiation(self) -> None:
        self._status = SessionStatus.DURING
        self._initiating_packet = MessagePacket(
            tag=self.tag,
            auth_tag=get_random_auth_tag(),
            encrypted_message=get_random_encrypted_data(),
        )
        await self._outbound_packet_send_channel.send(NetworkPacket(
            packet=self._initiating_packet,
            endpoint=self.remote_endpoint,
        ))

    async def receive_handshake_response(self,
                                         packet: HandshakeResponse,
                                         ) -> Tuple[SessionKeys, keys.PublicKey]:
        if not isinstance(packet, HandshakeResponse):
            raise Exception(f"Unhandled packet type: {type(packet)}")
        if not secrets.compare_digest(packet.token, self._initiating_packet.auth_tag):
            raise ValidationError("Mismatch between token")

        self.logger.debug('%s: receiving handshake response', self)

        # compute session keys
        ephemeral_private_key = keys.PrivateKey(secrets.token_bytes(32))

        self.remote_public_key = packet.public_key
        self.remote_public_key = packet.header.public_key
        expected_remote_node_id = public_key_to_node_id(self.remote_public_key)
        if expected_remote_node_id != self.remote_node_id:
            raise ValidationError(
                f"Remote node ids does not match expected node id: "
                f"{self.remote_node_id} != {self.remote_node_id}"
            )

        session_keys = compute_session_keys(
            local_private_key=ephemeral_private_key,
            remote_public_key=packet.public_key,
            local_node_id=self.local_node_id,
            remote_node_id=self.remote_node_id,
            id_nonce=packet.id_nonce,
            is_initiator=True,
        )
        return session_keys, ephemeral_private_key.public_key

    async def send_handshake_completion(self,
                                        session_keys: SessionKeys,
                                        ephemeral_public_key: keys.PublicKey,
                                        response_packet: HandshakeResponse) -> None:
        self.logger.debug('%s: sending handshake completion', self)

        # prepare response packet
        id_nonce_signature = create_id_nonce_signature(
            id_nonce=response_packet.id_nonce,
            ephemeral_public_key=ephemeral_public_key,
            private_key=self.private_key,
        )

        complete_handshake_packet = CompleteHandshakePacket.prepare(
            tag=self.tag,
            auth_tag=get_random_auth_tag(),
            id_nonce=response_packet.id_nonce,
            message=self._initial_message,
            initiator_key=session_keys.encryption_key,
            id_nonce_signature=id_nonce_signature,
            auth_response_key=session_keys.auth_response_key,
            ephemeral_public_key=ephemeral_public_key,
            public_key=self.private_key.public_key,
        )
        await self._outbound_packet_send_channel.send(NetworkPacket(
            packet=complete_handshake_packet,
            endpoint=self.remote_endpoint,
        ))


#
# Receipient
#
class SessionRecipient(BaseSession):
    is_initiator = False

    #
    # Message API
    #
    async def handle_outbound_message(self, message: MessageAPI[sedes.Serializable]) -> None:
        if self.is_handshake_complete:
            self.logger.debug("%s: sending message: %s", self, message)
            packet = MessagePacket.prepare(
                tag=self.tag,
                auth_tag=get_random_auth_tag(),
                message=message,
                key=self._session_keys.encryption_key,
            )
            await self._outbound_packet_send_channel.send(NetworkPacket(
                packet=packet,
                endpoint=self.remote_endpoint,
            ))
        elif self.is_before_handshake:
            self.logger.debug(
                "%s: outbound message before handshake: %s",
                self,
                message,
            )
            raise NotImplementedError
        elif self.is_during_handshake:
            self.logger.debug(
                '%s: handshake in progress: placing outbound message in queue: %s',
                self,
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
        self.last_message_at = time.monotonic()

        if self.is_handshake_complete:
            if isinstance(packet, MessagePacket):
                payload = packet.decrypt_payload(self._session_keys.decryption_key)
                message = Message(
                    payload=payload,
                    node=self.remote_node,
                )
                self.logger.debug(
                    '%s: processed inbound message packet: %s',
                    self,
                    message,
                )
                await self._inbound_message_send_channel.send(message)
            else:
                self.logger.debug(
                    '%s: Ignoring packet of type %s received after handshake complete',
                    self,
                    type(packet),
                )
        elif self.is_before_handshake:
            if isinstance(packet, MessagePacket):
                await self.receive_handshake_initiation(packet)
                await self.send_handshake_response(packet)
            else:
                # TODO: full buffer handling
                # TODO: manage buffer...
                self._inbound_packet_buffer_channels[0].send_nowait(packet)
        elif self.is_during_handshake:
            if isinstance(packet, CompleteHandshakePacket):
                self._session_keys = await self.receive_handshake_completion(packet)
                self._status = SessionStatus.AFTER
                await self._events.handshake_complete.trigger(self)
            else:
                self._inbound_packet_buffer_channels[0].send_nowait(packet)
        else:
            raise Exception("Invalid state")

    async def receive_handshake_initiation(self, packet: MessagePacket) -> None:
        self.logger.debug('%s: received handshake initiation', self)
        self._status = SessionStatus.DURING

    async def send_handshake_response(self, initiation_packet: MessagePacket) -> None:
        self.logger.debug('%s: sending handshake response', self)
        magic = compute_handshake_response_magic(self.remote_node_id)
        self.handshake_response_packet = HandshakeResponse(
            tag=self.tag,
            magic=magic,
            token=initiation_packet.auth_tag,
            id_nonce=get_random_id_nonce(),
            public_key=self.private_key.public_key,
        )
        await self._outbound_packet_send_channel.send(NetworkPacket(
            packet=self.handshake_response_packet,
            endpoint=self.remote_endpoint,
        ))

    async def receive_handshake_completion(self, packet: CompleteHandshakePacket) -> SessionKeys:
        self.logger.debug('%s: received handshake completion', self)
        remote_node_id = recover_source_id_from_tag(
            packet.tag,
            self.local_node_id,
        )
        if remote_node_id != self.remote_node_id:
            raise ValidationError(
                f"Remote node ids do not match: {remote_node_id} != {self.remote_node_id}"
            )

        self.remote_public_key = packet.header.public_key
        expected_remote_node_id = public_key_to_node_id(self.remote_public_key)
        if expected_remote_node_id != remote_node_id:
            raise ValidationError(
                f"Remote node ids does not match expected node id: "
                f"{remote_node_id} != {self.remote_node_id}"
            )

        ephemeral_public_key = packet.header.ephemeral_public_key

        session_keys = compute_session_keys(
            local_private_key=self.private_key,
            remote_public_key=ephemeral_public_key,
            local_node_id=self.local_node_id,
            remote_node_id=self.remote_node_id,
            id_nonce=self.handshake_response_packet.id_nonce,
            is_initiator=False,
        )

        self.decrypt_and_validate_auth_response(
            packet,
            session_keys.auth_response_key,
            self.handshake_response_packet.id_nonce,
        )
        payload = self.decrypt_and_validate_message(
            packet,
            session_keys.decryption_key,
        )
        message = Message(
            payload=payload,
            node=self.remote_node,
        )
        await self._inbound_message_send_channel.send(message)
        return session_keys

    def decrypt_and_validate_message(self,
                                     complete_handshake_packet: CompleteHandshakePacket,
                                     decryption_key: AES128Key
                                     ) -> sedes.Serializable:
        try:
            return complete_handshake_packet.decrypt_payload(decryption_key)
        except DecryptionError as error:
            raise HandshakeFailure(
                "Failed to decrypt message in AuthHeader packet with newly established session keys"
            ) from error
        except ValidationError as error:
            raise HandshakeFailure("Received invalid message") from error

    def decrypt_and_validate_auth_response(self,
                                           complete_handshake_packet: CompleteHandshakePacket,
                                           auth_response_key: AES128Key,
                                           id_nonce: IDNonce,
                                           ) -> None:
        try:
            id_nonce_signature = complete_handshake_packet.decrypt_auth_response(auth_response_key)
        except DecryptionError as error:
            raise HandshakeFailure("Unable to decrypt auth response") from error
        except ValidationError as error:
            raise HandshakeFailure("Invalid auth response content") from error

        try:
            validate_id_nonce_signature(
                signature=id_nonce_signature,
                id_nonce=id_nonce,
                ephemeral_public_key=complete_handshake_packet.header.ephemeral_public_key,
                public_key=complete_handshake_packet.header.public_key,
            )
        except ValidationError as error:
            raise HandshakeFailure("Invalid id nonce signature in auth response") from error


def validate_id_nonce_signature(*,
                                id_nonce: IDNonce,
                                ephemeral_public_key: bytes,
                                signature: keys.NonRecoverableSignature,
                                public_key: keys.PublicKey,
                                ) -> None:
    signature_input = create_id_nonce_signature_input(
        id_nonce=id_nonce,
        ephemeral_public_key=ephemeral_public_key,
    )
    validate_signature(
        message_hash=signature_input,
        signature=signature,
        public_key=public_key,
    )


def validate_signature(*,
                       message_hash: bytes,
                       signature: keys.NonRecoverableSignature,
                       public_key: keys.PublicKey,
                       ) -> None:

    if not signature.verify_msg_hash(message_hash, public_key):
        raise ValidationError(
            f"Signature {encode_hex(signature)} is not valid for message hash "
            f"{encode_hex(message_hash)} and public key {encode_hex(public_key)}"
        )
