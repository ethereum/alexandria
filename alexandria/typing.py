from typing import NewType


AES128Key = NewType("AES128Key", bytes)

NodeID = NewType('NodeID', int)

Tag = NewType("Tag", bytes)

Nonce = NewType("Nonce", bytes)
IDNonce = NewType("IDNonce", bytes)

# Skip Graph key
Key = NewType('Key', int)
