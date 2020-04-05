import math
import random

import pytest
import trio

from alexandria.constants import CHUNK_MAX_SIZE
from alexandria.payloads import Retrieve, Chunk


@pytest.mark.trio
async def test_client_retrieve_content_single_response(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        data = b'unicorns-and-rainbows'

        with bob.message_dispatcher.subscribe(Retrieve) as retrieve_subscription:
            async def _handle_retrieve():
                with trio.fail_after(1):
                    message = await retrieve_subscription.receive()

                assert isinstance(message.payload, Retrieve)
                await bob.send_chunks(
                    message.node,
                    request_id=message.payload.request_id,
                    data=data,
                )

            nursery.start_soon(_handle_retrieve)

            with trio.fail_after(1):
                messages = await alice.retrieve(bob.local_node, key=b'key')

            assert len(messages) == 1
            message = messages[0]

            assert isinstance(message.payload, Chunk)
            payload = message.payload
            assert payload.total == 1
            assert payload.index == 0
            assert payload.data == data


@pytest.mark.trio
async def test_client_retrieve_content_multi_response(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        data = bytes(bytearray((random.randint(0, 255) for _ in range(4096))))
        num_chunks = int(math.ceil(len(data) / CHUNK_MAX_SIZE))

        with bob.message_dispatcher.subscribe(Retrieve) as retrieve_subscription:
            async def _handle_retrieve():
                with trio.fail_after(1):
                    message = await retrieve_subscription.receive()

                assert isinstance(message.payload, Retrieve)
                await bob.send_chunks(
                    message.node,
                    request_id=message.payload.request_id,
                    data=data,
                )

            nursery.start_soon(_handle_retrieve)

            with trio.fail_after(1):
                messages = await alice.retrieve(bob.local_node, key=b'key')

            assert len(messages) == num_chunks
            for index, message in enumerate(messages):
                assert isinstance(message.payload, Chunk)
                assert message.payload.total == num_chunks
                assert message.payload.index == index

            recovered_data = b''.join((
                message.payload.data for message in messages
            ))
            assert recovered_data == data
