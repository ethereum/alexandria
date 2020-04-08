import itertools
import pathlib

import rlp
import trio

from web3 import Web3
from web3.providers import IPCProvider

from alexandria.logging import setup_logging


class BlockHeader(rlp.Serializable):
    fields = [
        ('parent_hash', hash32),
        ('uncles_hash', hash32),
        ('coinbase', address),
        ('state_root', trie_root),
        ('transaction_root', trie_root),
        ('receipt_root', trie_root),
        ('bloom', uint256),
        ('difficulty', big_endian_int),
        ('block_number', big_endian_int),
        ('gas_limit', big_endian_int),
        ('gas_used', big_endian_int),
        ('timestamp', big_endian_int),
        ('extra_data', binary),
        ('mix_hash', binary),
        ('nonce', Binary(8, allow_empty=True))
    ]

    @overload  # noqa: F811
    def __init__(self,
                 difficulty: int,
                 block_number: BlockNumber,
                 gas_limit: int,
                 timestamp: int=None,
                 coinbase: Address=ZERO_ADDRESS,
                 parent_hash: Hash32=ZERO_HASH32,
                 uncles_hash: Hash32=EMPTY_UNCLE_HASH,
                 state_root: Hash32=BLANK_ROOT_HASH,
                 transaction_root: Hash32=BLANK_ROOT_HASH,
                 receipt_root: Hash32=BLANK_ROOT_HASH,
                 bloom: int=0,
                 gas_used: int=0,
                 extra_data: bytes=b'',
                 mix_hash: Hash32=ZERO_HASH32,
                 nonce: bytes=GENESIS_NONCE) -> None:
        ...

    def __init__(self,              # type: ignore  # noqa: F811
                 difficulty: int,
                 block_number: BlockNumber,
                 gas_limit: int,
                 timestamp: int,
                 coinbase: Address,
                 parent_hash: Hash32,
                 uncles_hash: Hash32,
                 state_root: Hash32,
                 transaction_root: Hash32,
                 receipt_root: Hash32,
                 bloom: int,
                 gas_used: int,
                 extra_data: bytes,
                 mix_hash: Hash32,
                 nonce: bytes=GENESIS_NONCE) -> None:
        if timestamp is None:
            timestamp = int(time.time())
        super().__init__(
            parent_hash=parent_hash,
            uncles_hash=uncles_hash,
            coinbase=coinbase,
            state_root=state_root,
            transaction_root=transaction_root,
            receipt_root=receipt_root,
            bloom=bloom,
            difficulty=difficulty,
            block_number=block_number,
            gas_limit=gas_limit,
            gas_used=gas_used,
            timestamp=timestamp,
            extra_data=extra_data,
            mix_hash=mix_hash,
            nonce=nonce,
        )

    def __str__(self) -> str:
        return '<BlockHeader #{0} {1}>'.format(
            self.block_number,
            encode_hex(self.hash)[2:10],
        )

    _hash = None

    @property
    def hash(self) -> Hash32:
        if self._hash is None:
            self._hash = keccak(rlp.encode(self))
        return self._hash

    @property
    def mining_hash(self) -> Hash32:
        return keccak(rlp.encode(self[:-2], MiningHeader))

    @property
    def hex_hash(self) -> str:
        return encode_hex(self.hash)

    @classmethod
    def from_parent(cls,
                    parent: BlockHeaderAPI,
                    gas_limit: int,
                    difficulty: int,
                    timestamp: int,
                    coinbase: Address=ZERO_ADDRESS,
                    nonce: bytes=None,
                    extra_data: bytes=None,
                    transaction_root: bytes=None,
                    receipt_root: bytes=None) -> BlockHeaderAPI:
        """
        Initialize a new block header with the `parent` header as the block's
        parent hash.
        """
        header_kwargs: Dict[str, HeaderParams] = {
            'parent_hash': parent.hash,
            'coinbase': coinbase,
            'state_root': parent.state_root,
            'gas_limit': gas_limit,
            'difficulty': difficulty,
            'block_number': parent.block_number + 1,
            'timestamp': timestamp,
        }
        if nonce is not None:
            header_kwargs['nonce'] = nonce
        if extra_data is not None:
            header_kwargs['extra_data'] = extra_data
        if transaction_root is not None:
            header_kwargs['transaction_root'] = transaction_root
        if receipt_root is not None:
            header_kwargs['receipt_root'] = receipt_root

        header = cls(**header_kwargs)
        return header

    def create_execution_context(self,
                                 prev_hashes: Iterable[Hash32],
                                 chain_context: ChainContext) -> ExecutionContext:

        return ExecutionContext(
            coinbase=self.coinbase,
            timestamp=self.timestamp,
            block_number=self.block_number,
            difficulty=self.difficulty,
            gas_limit=self.gas_limit,
            prev_hashes=prev_hashes,
            chain_id=chain_context.chain_id,
        )

    @property
    def is_genesis(self) -> bool:
        # if removing the block_number == 0 test, consider the validation consequences.
        # validate_header stops trying to check the current header against a parent header.
        # Can someone trick us into following a high difficulty header with genesis parent hash?
        return self.parent_hash == GENESIS_PARENT_HASH and self.block_number == 0


def get_w3(ipc_path: pathlib.Path = None) -> Web3:
    if ipc_path is None:
        ipc_path = get_xdg_alexandria_root() / 'jsonrpc.ipc'

    provider = IPCProvider(str(ipc_path))
    w3 = Web3(provider=provider, modules={'alexandria': (AlexandriaModule,)})
    return w3


async def exfiltrate_headers(w3: Web3,
                             export_db: DurableDatabaseAPI,
                             start_at: int):
    (
        header_range_send_channel,
        header_range_receive_channel,
    ) = trio.open_memory_channel['HeaderRange'][0]


async def retrieve_header(w3: Web3, block_number: int) -> BlockHeader:
    ...


if __name__ == '__main__':
    setup_logging()
    trio.run(exfiltrate_headers)
