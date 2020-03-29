# Number of bits in the keyspace
KEY_BIT_SIZE = 256
KEY_BYTE_SIZE = KEY_BIT_SIZE // 8

# Max size of packets.
MAX_PACKET_SIZE = 1280

# Buffer size used for incoming discovery UDP datagrams (must be larger than
# MAX_PACKET_SIZE)
DATAGRAM_BUFFER_SIZE = MAX_PACKET_SIZE * 2

# info bytes for key derivation
HKDF_INFO = b"alexandria key agreement"

#  The name of the auth scheme.
AUTH_SCHEME_NAME = b"gcm"

# size of an AES218 key
AES128_KEY_SIZE = 16

# The set of all single byte values for quick single byte conversions.
ALL_BYTES = tuple(bytes([i]) for i in range(256))
