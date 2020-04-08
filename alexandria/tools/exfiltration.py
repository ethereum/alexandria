import argparse
import logging
from typing import Optional

from eth_typing import Address, BlockNumber, Hash32
from eth_utils import keccak, encode_hex, humanize_hash, to_canonical_address, big_endian_to_int
import rlp
from rlp import sedes
import trio

from web3 import Web3

from alexandria.logging import setup_logging
from alexandria.w3 import get_w3

logger = logging.getLogger('alexandria.tools.exfiltration')


address = sedes.Binary.fixed_length(20, allow_empty=True)
hash32 = sedes.Binary.fixed_length(32)
uint32 = sedes.BigEndianInt(32)
uint256 = sedes.BigEndianInt(256)
trie_root = sedes.Binary.fixed_length(32, allow_empty=True)


class BlockHeader(rlp.Serializable):  # type: ignore
    fields = [
        ('parent_hash', hash32),
        ('uncles_hash', hash32),
        ('coinbase', address),
        ('state_root', trie_root),
        ('transaction_root', trie_root),
        ('receipt_root', trie_root),
        ('bloom', uint256),
        ('difficulty', sedes.big_endian_int),
        ('block_number', sedes.big_endian_int),
        ('gas_limit', sedes.big_endian_int),
        ('gas_used', sedes.big_endian_int),
        ('timestamp', sedes.big_endian_int),
        ('extra_data', sedes.binary),
        ('mix_hash', sedes.binary),
        ('nonce', sedes.Binary(8, allow_empty=True))
    ]

    def __init__(self,
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
                 nonce: bytes) -> None:
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
            humanize_hash(self.hash),
        )

    _hash: Optional[Hash32] = None

    @property
    def hash(self) -> Hash32:
        if self._hash is None:
            self._hash = Hash32(keccak(rlp.encode(self)))
        return self._hash

    @property
    def hex_hash(self) -> str:
        return encode_hex(self.hash)


def get_header_key(genesis_hash: Hash32, header: BlockHeader) -> bytes:
    return b'%s/header/%s' % (genesis_hash, header.hash)


async def exfiltrate_headers(w3_eth: Web3,
                             w3_alexandria: Web3,
                             start_at: int,
                             end_at: int,
                             is_ephemeral: bool) -> None:
    genesis = await retrieve_header(w3_eth, 0)
    async with trio.open_nursery() as nursery:
        (
            send_channel,
            receive_channel,
        ) = trio.open_memory_channel[BlockHeader](0)
        logger.info('Starting exfiltration')
        nursery.start_soon(retrieve_headers, w3_eth, start_at, end_at, send_channel)
        async with receive_channel:
            async for header in receive_channel:
                await push_header_to_alexandria(w3_alexandria, genesis.hash, header, is_ephemeral)
                logger.info('Exfiltrated #%s', header.block_number)


async def push_header_to_alexandria(w3: Web3,
                                    genesis_hash: Hash32,
                                    header: BlockHeader,
                                    is_ephemeral: bool) -> None:
    await trio.to_thread.run_sync(
        _push_header_to_alexandria,
        w3,
        genesis_hash,
        header,
    )


def _push_header_to_alexandria(w3: Web3,
                               genesis_hash: Hash32,
                               header: BlockHeader,
                               is_ephemeral: bool) -> None:
    key = get_header_key(genesis_hash, header)
    header_rlp = rlp.encode(header)
    w3.alexandria.add_content(key, header_rlp, is_ephemeral=is_ephemeral)  # type: ignore
    logger.debug(
        'Header #%d pushed to Alexandria under key: %s',
        header.block_number,
        encode_hex(key),
    )


async def retrieve_headers(w3: Web3,
                           start_at: int,
                           end_at: int,
                           send_channel: trio.abc.SendChannel[BlockHeader],
                           request_rate: int = 3) -> None:
    semaphor = trio.Semaphore(request_rate, max_value=request_rate)

    async def _fetch(block_number: int) -> None:
        header = await retrieve_header(w3, block_number)
        semaphor.release()
        await send_channel.send(header)

    async with send_channel:
        async with trio.open_nursery() as nursery:
            logger.debug('Starting retrieval of headers %d-%d', start_at, end_at)
            for block_number in range(start_at, end_at):
                await semaphor.acquire()
                nursery.start_soon(_fetch, block_number)


async def retrieve_header(w3: Web3, block_number: int) -> BlockHeader:
    logger.debug('Retrieving header #%d', block_number)
    fancy_header = await trio.to_thread.run_sync(w3.eth.getBlock, block_number)
    header = BlockHeader(
        difficulty=fancy_header['difficulty'],
        block_number=fancy_header['number'],
        gas_limit=fancy_header['gasLimit'],
        timestamp=fancy_header['timestamp'],
        coinbase=to_canonical_address(fancy_header['miner']),
        parent_hash=Hash32(fancy_header['parentHash']),
        uncles_hash=Hash32(fancy_header['sha3Uncles']),
        state_root=Hash32(fancy_header['stateRoot']),
        transaction_root=Hash32(fancy_header['transactionsRoot']),
        receipt_root=Hash32(fancy_header['receiptsRoot']),  # type: ignore
        bloom=big_endian_to_int(bytes(fancy_header['logsBloom'])),
        gas_used=fancy_header['gasUsed'],
        extra_data=bytes(fancy_header['extraData']),
        mix_hash=Hash32(fancy_header['mixHash']),
        nonce=bytes(fancy_header['nonce']),
    )
    if header.hash != Hash32(fancy_header['hash']):
        raise ValueError(
            f"Reconstructed header hash does not match expected: "
            f"expected={encode_hex(fancy_header['hash'])}  actual={header.hex_hash}"
        )
    return header


parser = argparse.ArgumentParser(description='Block Header Exfiltration')
parser.add_argument('--start-at', type=int)
parser.add_argument('--end-at', type=int)
parser.add_argument('--durable', action="store_true", default=False)


if __name__ == '__main__':
    args = parser.parse_args()
    setup_logging(logging.INFO)
    w3_alexandria = get_w3()
    from web3.auto.ipc import w3 as w3_eth
    trio.run(
        exfiltrate_headers,
        w3_eth,
        w3_alexandria,
        args.start_at,
        args.end_at,
        not args.durable,
    )
