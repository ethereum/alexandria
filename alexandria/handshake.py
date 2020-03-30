import hashlib
from typing import (
    NamedTuple,
    NewType,
    Tuple,
)

from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.backends import default_backend as cryptography_default_backend

import coincurve

from eth_keys import keys

from alexandria.constants import ID_NONCE_SIGNATURE_PREFIX, HKDF_INFO, AES128_KEY_SIZE
from alexandria.typing import NodeID, IDNonce


def ecdh_agree(private_key: keys.PrivateKey, public_key: keys.PublicKey) -> bytes:
    """Perform the ECDH key agreement.

    The public key is expected in uncompressed format and the resulting secret point will be
    formatted as a 0x02 or 0x03 prefix (depending on the sign of the secret's y component)
    followed by 32 bytes of the x component.
    """
    public_key_compressed = public_key.to_compressed_bytes()
    public_key_coincurve = coincurve.keys.PublicKey(public_key_compressed)
    secret_coincurve = public_key_coincurve.multiply(private_key.to_bytes())
    return secret_coincurve.format()


AES128Key = NewType("AES128Key", bytes)


class SessionKeys(NamedTuple):
    encryption_key: AES128Key
    decryption_key: AES128Key
    auth_response_key: AES128Key


def hkdf_expand_and_extract(secret: bytes,
                            initiator_node_id: NodeID,
                            recipient_node_id: NodeID,
                            id_nonce: IDNonce,
                            ) -> Tuple[bytes, bytes, bytes]:
    info = b"".join((
        HKDF_INFO,
        initiator_node_id.to_bytes(32, 'big'),
        recipient_node_id.to_bytes(32, 'big'),
    ))

    hkdf = HKDF(
        algorithm=SHA256(),
        length=3 * AES128_KEY_SIZE,
        salt=id_nonce,
        info=info,
        backend=cryptography_default_backend(),
    )
    expanded_key = hkdf.derive(secret)

    if len(expanded_key) != 3 * AES128_KEY_SIZE:
        raise Exception("Invariant: Secret is expanded to three AES128 keys")

    initiator_key = expanded_key[:AES128_KEY_SIZE]
    recipient_key = expanded_key[AES128_KEY_SIZE:2 * AES128_KEY_SIZE]
    auth_response_key = expanded_key[2 * AES128_KEY_SIZE:3 * AES128_KEY_SIZE]

    return initiator_key, recipient_key, auth_response_key


def compute_session_keys(*,
                         local_private_key: keys.PrivateKey,
                         remote_public_key: keys.PublicKey,
                         local_node_id: NodeID,
                         remote_node_id: NodeID,
                         id_nonce: IDNonce,
                         is_initiator: bool
                         ) -> SessionKeys:
    secret = ecdh_agree(local_private_key, remote_public_key)

    if is_initiator:
        initiator_node_id, recipient_node_id = local_node_id, remote_node_id
    else:
        initiator_node_id, recipient_node_id = remote_node_id, local_node_id

    initiator_key, recipient_key, auth_response_key = hkdf_expand_and_extract(
        secret,
        initiator_node_id,
        recipient_node_id,
        id_nonce,
    )

    if is_initiator:
        encryption_key, decryption_key = initiator_key, recipient_key
    else:
        encryption_key, decryption_key = recipient_key, initiator_key

    return SessionKeys(
        encryption_key=AES128Key(encryption_key),
        decryption_key=AES128Key(decryption_key),
        auth_response_key=AES128Key(auth_response_key),
    )


def create_id_nonce_signature_input(id_nonce: IDNonce,
                                    ephemeral_public_key: keys.PublicKey,
                                    ) -> bytes:
    preimage = b"".join((
        ID_NONCE_SIGNATURE_PREFIX,
        id_nonce,
        ephemeral_public_key.to_compressed_bytes(),
    ))
    return hashlib.sha256(preimage).digest()


def create_id_nonce_signature(id_nonce: IDNonce,
                              ephemeral_public_key: keys.PublicKey,
                              private_key: keys.PrivateKey,
                              ) -> bytes:
    signature_input = create_id_nonce_signature_input(
        id_nonce=id_nonce,
        ephemeral_public_key=ephemeral_public_key,
    )
    signature = private_key.sign_msg_hash_non_recoverable(signature_input)
    return bytes(signature)
