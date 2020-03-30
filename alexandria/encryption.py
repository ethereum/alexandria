from cryptography.hazmat.primitives.ciphers.aead import (
    AESGCM,
)
from cryptography.exceptions import (
    InvalidTag,
)

from eth_utils import ValidationError

from alexandria.constants import AES128_KEY_SIZE, NONCE_SIZE
from alexandria.typing import AES128Key, Nonce
from alexandria.exceptions import DecryptionError


def validate_aes128_key(key: AES128Key) -> None:
    if len(key) != AES128_KEY_SIZE:
        raise ValidationError('Invalid key size')


def validate_nonce(nonce: bytes) -> None:
    if len(nonce) != NONCE_SIZE:
        raise ValidationError('Invalid nonce size')


def aesgcm_encrypt(key: AES128Key,
                   nonce: Nonce,
                   plain_text: bytes,
                   authenticated_data: bytes
                   ) -> bytes:
    validate_aes128_key(key)
    validate_nonce(nonce)

    aesgcm = AESGCM(key)
    cipher_text = aesgcm.encrypt(nonce, plain_text, authenticated_data)
    return cipher_text


def aesgcm_decrypt(key: AES128Key,
                   nonce: Nonce,
                   cipher_text: bytes,
                   authenticated_data: bytes
                   ) -> bytes:
    validate_aes128_key(key)
    validate_nonce(nonce)

    aesgcm = AESGCM(key)
    try:
        plain_text = aesgcm.decrypt(nonce, cipher_text, authenticated_data)
    except InvalidTag as error:
        raise DecryptionError() from error
    else:
        return plain_text
