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

# size of an AES218 key
AES128_KEY_SIZE = 16
