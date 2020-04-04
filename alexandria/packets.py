import hashlib
import secrets
from typing import NamedTuple, Type

from eth_keys import keys
from eth_typing import Hash32
from eth_utils import ValidationError

import ssz
from ssz import sedes

from alexandria.abc import MessageAPI, PacketAPI, RegistryAPI
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
from alexandria.encryption import aesgcm_encrypt, aesgcm_decrypt
from alexandria.messages import default_registry
from alexandria.sedes import byte_list
from alexandria.typing import Tag, NodeID, Nonce, IDNonce, AES128Key


bytes12 = sedes.ByteVector(12)
bytes33 = sedes.ByteVector(33)
bytes64 = sedes.ByteVector(64)


class AuthHeader(NamedTuple):
    auth_tag: Nonce
    id_nonce: IDNonce
    auth_scheme_name: bytes
    ephemeral_public_key: keys.PublicKey
    public_key: keys.PublicKey
    encrypted_auth_response: bytes


AUTH_HEADER_SEDES = ssz.Container((
    bytes12,  # auth_tag
    sedes.bytes32,  # id_nonce
    byte_list,  # auth_scheme
    bytes33,  # ephemeral public key (compressed)
    bytes33,  # public key (compressed)
    byte_list,  # encrypted_auth_response
))
COMPLETE_HANDSHAKE_PACKET_SEDES = ssz.Container((
    sedes.bytes32,  # tag
    AUTH_HEADER_SEDES,  # header
    byte_list,  # encrypted_message
))


AUTH_RESPONSE_SEDES = ssz.Container((
    sedes.uint8,
    bytes64,
))


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

    def __str__(self) -> str:
        return "CompleteHandshakePacket"

    @classmethod
    def prepare(cls: Type["CompleteHandshakePacket"],
                *,
                tag: Tag,
                auth_tag: Nonce,
                id_nonce: IDNonce,
                message: MessageAPI[sedes.Serializable],
                initiator_key: AES128Key,
                id_nonce_signature: bytes,
                auth_response_key: AES128Key,
                ephemeral_public_key: keys.PublicKey,
                public_key: keys.PublicKey,
                ) -> "CompleteHandshakePacket":
        encrypted_auth_response = compute_encrypted_auth_response(
            auth_response_key=auth_response_key,
            id_nonce_signature=id_nonce_signature,
        )
        header = AuthHeader(
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
            header=header,
            encrypted_message=encrypted_message,
        )

    def decrypt_payload(self,
                        key: AES128Key,
                        message_registry: RegistryAPI = default_registry,
                        ) -> ssz.Serializable:
        return _decrypt_payload(
            key=key,
            auth_tag=self.header.auth_tag,
            encrypted_message=self.encrypted_message,
            authenticated_data=self.tag,
            message_registry=message_registry,
        )

    def to_wire_bytes(self) -> bytes:
        return bytes(ssz.encode(
            (
                self.tag,
                (
                    self.header.auth_tag,
                    self.header.id_nonce,
                    self.header.auth_scheme_name,
                    self.header.ephemeral_public_key.to_compressed_bytes(),
                    self.header.public_key.to_compressed_bytes(),
                    self.header.encrypted_auth_response,
                ),
                self.encrypted_message,
            ),
            sedes=COMPLETE_HANDSHAKE_PACKET_SEDES,
        ))

    @classmethod
    def from_wire_bytes(cls: Type["CompleteHandshakePacket"],
                        data: bytes) -> "CompleteHandshakePacket":
        tag, header_as_tuple, encrypted_message = ssz.decode(data, COMPLETE_HANDSHAKE_PACKET_SEDES)
        (
            auth_tag,
            id_nonce,
            auth_scheme_name,
            ephemeral_public_key_bytes,
            public_key_bytes,
            encrypted_auth_response,
        ) = header_as_tuple
        ephemeral_public_key = keys.PublicKey.from_compressed_bytes(ephemeral_public_key_bytes)
        public_key = keys.PublicKey.from_compressed_bytes(public_key_bytes)
        header = AuthHeader(
            auth_tag,
            id_nonce,
            auth_scheme_name,
            ephemeral_public_key,
            public_key=public_key,
            encrypted_auth_response=encrypted_auth_response,
        )
        return cls(
            tag=tag,
            header=header,
            encrypted_message=encrypted_message,
        )

    def decrypt_auth_response(self, auth_response_key: AES128Key) -> keys.NonRecoverableSignature:
        """Extract id nonce signature from complete handshake packet."""
        plain_text = aesgcm_decrypt(
            key=auth_response_key,
            nonce=ZERO_NONCE,
            cipher_text=self.header.encrypted_auth_response,
            authenticated_data=b"",
        )

        version, id_nonce_signature = ssz.decode(plain_text, AUTH_RESPONSE_SEDES)

        if version != AUTH_RESPONSE_VERSION:
            raise ValidationError(
                f"Expected auth response version {AUTH_RESPONSE_VERSION}, but got {version}"
            )

        signature = keys.NonRecoverableSignature(id_nonce_signature)

        return signature


HANDSHAKE_RESPONSE_SEDES = sedes.Container((
    sedes.bytes32,
    sedes.bytes32,
    bytes12,
    sedes.bytes32,
    bytes33,
))


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

    def __str__(self) -> str:
        return "HandshakeResponse"

    def to_wire_bytes(self) -> bytes:
        return bytes(ssz.encode(
            (self.tag, self.magic, self.token, self.id_nonce, self.public_key.to_compressed_bytes()),  # noqa: E501
            sedes=HANDSHAKE_RESPONSE_SEDES,
        ))

    @classmethod
    def from_wire_bytes(cls: Type["HandshakeResponse"], data: bytes) -> "HandshakeResponse":
        tag, magic, token, id_nonce, public_key_compressed_bytes = ssz.decode(
            data,
            HANDSHAKE_RESPONSE_SEDES,
        )
        public_key = keys.PublicKey.from_compressed_bytes(public_key_compressed_bytes)
        return cls(
            tag=tag,
            magic=magic,
            token=token,
            id_nonce=id_nonce,
            public_key=public_key,
        )


MESSAGE_PACKET_SEDES = ssz.Container((
    sedes.bytes32,
    bytes12,
    byte_list,
))


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

    def __str__(self) -> str:
        return "MessagePacket"

    @classmethod
    def prepare(cls,
                *,
                tag: Tag,
                auth_tag: Nonce,
                message: MessageAPI[sedes.Serializable],
                key: AES128Key,
                ) -> "MessagePacket":
        encrypted_message = compute_encrypted_message(
            key=key,
            auth_tag=auth_tag,
            message=message,
            authenticated_data=tag,
        )
        return cls(
            tag=tag,
            auth_tag=auth_tag,
            encrypted_message=encrypted_message,
        )

    def to_wire_bytes(self) -> bytes:
        return bytes(ssz.encode(
            (self.tag, self.auth_tag, self.encrypted_message),
            sedes=MESSAGE_PACKET_SEDES,
        ))

    def decrypt_payload(self,
                        key: AES128Key,
                        message_registry: RegistryAPI = default_registry,
                        ) -> ssz.Serializable:
        return _decrypt_payload(
            key=key,
            auth_tag=self.auth_tag,
            encrypted_message=self.encrypted_message,
            authenticated_data=self.tag,
            message_registry=message_registry,
        )

    @classmethod
    def from_wire_bytes(cls: Type["MessagePacket"], data: bytes) -> "MessagePacket":
        tag, auth_tag, encrypted_message = ssz.decode(data, MESSAGE_PACKET_SEDES)
        return cls(
            tag=tag,
            auth_tag=auth_tag,
            encrypted_message=encrypted_message,
        )


#
# Packet decryption
#
def _decrypt_payload(key: AES128Key,
                     auth_tag: Nonce,
                     encrypted_message: bytes,
                     authenticated_data: bytes,
                     message_registry: RegistryAPI,
                     ) -> ssz.Serializable:
    plain_text = aesgcm_decrypt(
        key=key,
        nonce=auth_tag,
        cipher_text=encrypted_message,
        authenticated_data=authenticated_data,
    )

    try:
        message_id = plain_text[0]
    except IndexError:
        raise ValidationError("Decrypted message is empty")

    try:
        sedes = message_registry.get_sedes(message_id)
    except KeyError:
        raise ValidationError(f"Unknown message type {message_id}")

    try:
        message = ssz.decode(plain_text[1:], sedes)
    except ssz.DeserializationError as error:
        raise ValidationError("Encrypted message does not contain valid RLP") from error

    return message


#
# Packet encoding/decoding
#
def encode_packet(packet: PacketAPI) -> bytes:
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
    plain_text_auth_response = bytes(ssz.encode(
        (AUTH_RESPONSE_VERSION, id_nonce_signature),
        sedes=AUTH_RESPONSE_SEDES,
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
                              message: MessageAPI[sedes.Serializable],
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
