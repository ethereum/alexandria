import hashlib
import secrets
from typing import NamedTuple, Type

from eth_keys import keys
from eth_typing import Hash32

from alexandria.abc import MessageAPI, PacketAPI, TPacket
from alexandria.constants import (
    AUTH_SCHEME_NAME,
    AUTH_RESPONSE_VERSION,
    ALL_BYTES,
    ID_NONCE_SIZE,
    NONCE_SIZE,
    RANDOM_ENCRYPTED_DATA_SIZE,
    HANDSHAKE_RESPONSE_MAGIC_SUFFIX,
    ZERO_NONCE,
)
from alexandria.encryption import aesgcm_encrypt
from alexandria.typing import Tag, NodeID, Nonce, IDNonce, AES128Key


class AuthHeader(NamedTuple):
    auth_tag: Nonce
    id_nonce: IDNonce
    auth_scheme_name: bytes
    ephemeral_public_key: keys.PublicKey
    public_key: keys.PublicKey
    encrypted_auth_response: bytes


class CompleteHandshakePacket(PacketAPI):
    packet_id = 0

    tag: Tag
    header: AuthHeader
    encrypted_message: bytes

    def __init__(self,
                 tag: Tag,
                 header: AuthHeader,
                 encrypted_message: bytes,
                 ) -> None:
        self.tag = tag
        self.header = header
        self.encrypted_message = encrypted_message

    @classmethod
    def prepare(cls: Type[TPacket],
                *,
                tag: Tag,
                auth_tag: Nonce,
                id_nonce: IDNonce,
                message: MessageAPI,
                initiator_key: AES128Key,
                id_nonce_signature: bytes,
                auth_response_key: AES128Key,
                ephemeral_public_key: keys.PublicKey,
                public_key: keys.PublicKey,
                ) -> TPacket:
        encrypted_auth_response = compute_encrypted_auth_response(
            auth_response_key=auth_response_key,
            id_nonce_signature=id_nonce_signature,
        )
        auth_header = AuthHeader(
            auth_tag=auth_tag,
            id_nonce=id_nonce,
            auth_scheme_name=AUTH_SCHEME_NAME,
            ephemeral_public_key=ephemeral_public_key,
            public_key=public_key,
            encrypted_auth_response=encrypted_auth_response,
        )

        encrypted_message = compute_encrypted_message(
            key=initiator_key,
            auth_tag=auth_tag,
            message=message,
            authenticated_data=tag,
        )

        return cls(
            tag=tag,
            auth_header=auth_header,
            encrypted_message=encrypted_message,
        )

    def to_wire_bytes(self) -> bytes:
        raise NotImplementedError
        return b''.join((
            self.tag,
            ...,
            self.encrypted_message,
        ))

    @classmethod
    def from_wire_bytes(cls: Type[TPacket], data: bytes) -> TPacket:
        raise NotImplementedError


class HandshakeResponse(PacketAPI):
    packet_id = 1

    tag: Tag
    magic: Hash32
    token: Nonce
    id_nonce: IDNonce
    public_key: keys.PublicKey

    def __init__(self,
                 tag: Tag,
                 magic: Hash32,
                 token: Nonce,
                 id_nonce: IDNonce,
                 public_key: keys.PublicKey
                 ) -> None:
        self.tag = tag
        self.magic = magic
        self.token = token
        self.id_nonce = id_nonce
        self.public_key = public_key

    def to_wire_bytes(self) -> bytes:
        return b''.join((
            self.tag,
            self.magic,
            self.token,
            self.id_nonce,
            self.public_key.to_compressed_bytes(),
        ))

    @classmethod
    def from_wire_bytes(cls: Type[TPacket], data: bytes) -> TPacket:
        assert len(data) == 32 + 32 + NONCE_SIZE + ID_NONCE_SIZE + 33
        tag = data[:32]
        magic = data[32:64]
        token = data[64:64 + NONCE_SIZE]
        id_nonce = data[64 + NONCE_SIZE:64 + NONCE_SIZE + ID_NONCE_SIZE]
        compressed_public_key_bytes = data[-33:]
        public_key = keys.PublicKey.from_compressed_bytes(compressed_public_key_bytes)
        return cls(
            tag=tag,
            magic=magic,
            token=token,
            id_nonce=id_nonce,
            public_key=public_key,
        )


class MessagePacket(PacketAPI):
    packet_id = 2

    tag: Tag
    auth_tag: Nonce
    encrypted_message: bytes

    def __init__(self,
                 tag: Tag,
                 auth_tag: Nonce,
                 encrypted_message: bytes,
                 ) -> None:
        self.tag = tag
        self.auth_tag = auth_tag
        self.encrypted_message = encrypted_message

    def to_wire_bytes(self) -> bytes:
        return b''.join((
            self.tag,
            self.auth_tag,
            self.encrypted_message,
        ))

    @classmethod
    def from_wire_bytes(cls: Type[TPacket], data: bytes) -> TPacket:
        tag = data[:32]
        assert len(tag) == 32
        auth_tag = data[32:32 + NONCE_SIZE]
        assert len(auth_tag) == NONCE_SIZE
        encrypted_message = data[32 + NONCE_SIZE:]
        assert len(encrypted_message) > 0
        return cls(
            tag=tag,
            auth_tag=auth_tag,
            encrypted_message=encrypted_message,
        )


def encode_packet(packet: PacketAPI):
    return ALL_BYTES[packet.packet_id] + packet.to_wire_bytes()


def decode_packet(data: bytes) -> PacketAPI:
    packet_id = data[0]
    if packet_id == MessagePacket.packet_id:
        return MessagePacket.from_wire_bytes(data[1:])
    elif packet_id == HandshakeResponse.packet_id:
        return HandshakeResponse.from_wire_bytes(data[1:])
    elif packet_id == CompleteHandshakePacket.packet_id:
        return CompleteHandshakePacket.from_wire_bytes(data[1:])
    else:
        raise TypeError(f"Unknown packet id: {packet_id}")


#
# Packet Data helpers
#
def compute_encrypted_auth_response(auth_response_key: AES128Key,
                                    id_nonce_signature: bytes,
                                    ) -> bytes:
    plain_text_auth_response = b''.join((
        ALL_BYTES[AUTH_RESPONSE_VERSION],
        len(id_nonce_signature).to_bytes(2, 'big'),
        id_nonce_signature,
    ))

    encrypted_auth_response = aesgcm_encrypt(
        key=auth_response_key,
        nonce=ZERO_NONCE,
        plain_text=plain_text_auth_response,
        authenticated_data=b"",
    )
    return encrypted_auth_response


def compute_encrypted_message(key: AES128Key,
                              auth_tag: Nonce,
                              message: MessageAPI,
                              authenticated_data: bytes,
                              ) -> bytes:
    encrypted_message = aesgcm_encrypt(
        key=key,
        nonce=auth_tag,
        plain_text=message.to_bytes(),
        authenticated_data=authenticated_data,
    )
    return encrypted_message


def compute_handshake_response_magic(destination_node_id: NodeID) -> Hash32:
    preimage = destination_node_id.to_bytes(32, 'big') + HANDSHAKE_RESPONSE_MAGIC_SUFFIX
    return Hash32(hashlib.sha256(preimage).digest())


#
# Random packet data
#
def get_random_encrypted_data() -> bytes:
    return secrets.token_bytes(RANDOM_ENCRYPTED_DATA_SIZE)


def get_random_id_nonce() -> IDNonce:
    return IDNonce(secrets.token_bytes(ID_NONCE_SIZE))


def get_random_auth_tag() -> Nonce:
    return Nonce(secrets.token_bytes(NONCE_SIZE))
