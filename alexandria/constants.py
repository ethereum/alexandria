from .typing import Nonce


# Number of bits in the keyspace
KEY_BIT_SIZE = 256
KEY_BYTE_SIZE = KEY_BIT_SIZE // 8

# Max size of packets.
MAX_PACKET_SIZE = 1280

# Max size for the `nodes` payload of a `FoundNodes` message
FOUND_NODES_PAYLOAD_SIZE = MAX_PACKET_SIZE - 200

# The encoded size of a single encoded payload of [node_id, ip_address, port]
SINGLE_NODE_PAYLOAD_SIZE = 38

# The number of node payloads that can fit into a datagram
NODES_PER_PAYLOAD = FOUND_NODES_PAYLOAD_SIZE // 38

# The maximum size of a data chunk
CHUNK_MAX_SIZE = MAX_PACKET_SIZE - 32

# Buffer size used for incoming discovery UDP datagrams (must be larger than
# MAX_PACKET_SIZE)
DATAGRAM_BUFFER_SIZE = MAX_PACKET_SIZE * 2

# info bytes for key derivation
HKDF_INFO = b"alexandria key agreement"

#  The name of the auth scheme.
AUTH_SCHEME_NAME = b"gcm"

# version number used in auth response
AUTH_RESPONSE_VERSION = 4

# size of an AES218 key
AES128_KEY_SIZE = 16

# The set of all single byte values for quick single byte conversions.
ALL_BYTES = tuple(bytes([i]) for i in range(256))

HANDSHAKE_RESPONSE_MAGIC_SUFFIX = b"HANDSHAKE_RESPONSE"

ID_NONCE_SIGNATURE_PREFIX = b"discovery-id-nonce"

NONCE_SIZE = 12  # size of an AESGCM nonce
ZERO_NONCE = Nonce(b"\x00" * NONCE_SIZE)  # nonce used for the auth header packet

AUTH_TAG_SIZE = NONCE_SIZE

TAG_SIZE = 32  # size of the tag packet prefix

MAGIC_SIZE = 32  # size of the magic hash in the who are you packet

ID_NONCE_SIZE = 32  # size of the id nonce in who are you and auth tag packets

RANDOM_ENCRYPTED_DATA_SIZE = 12  # size of random data we send to initiate a handshake

#
# Timeouts
#
PING_TIMEOUT = 3
FIND_NODES_TIMEOUT = 5

MAX_CONTENT_KEY_SIZE = 256
MEGABYTE = 1048576  # 2**20
MAX_CONTENT_DATA_SIZE = MEGABYTE

KADEMLIA_PING_INTERVAL = 30  # interval of outgoing pings sent to maintain the routing table
KADEMLIA_LOOKUP_INTERVAL = 60  # intervals between lookups
KADEMLIA_ANNOUNCE_INTERVAL = 600  # 10 minutes
KADEMLIA_ANNOUNCE_CONCURRENCY = 3

HANDSHAKE_TIMEOUT = 5

SESSION_IDLE_TIMEOUT = 60
