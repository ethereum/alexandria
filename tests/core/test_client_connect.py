import logging
import pytest

import trio
from eth_utils import humanize_hash

from async_service import background_trio_service

from alexandria.messages import Ping, Message, Pong
from alexandria.tools.factories import ClientFactory, EndpointFactory

logger = logging.getLogger('alexandria.testing')


@pytest.mark.trio
async def test_client_connect():
    alice = ClientFactory()
    bob = ClientFactory()

    logger.info('ALICE: %s', humanize_hash(alice.local_node_id.to_bytes(32, 'big')))
    logger.info('BOB: %s', humanize_hash(bob.local_node_id.to_bytes(32, 'big')))

    async with background_trio_service(bob), background_trio_service(alice):
        await alice.wait_listening()
        await bob.wait_listening()

        alice_endpoint = EndpointFactory(ip_address='127.0.0.1', port=alice.listen_on.port)
        bob_endpoint = EndpointFactory(ip_address='127.0.0.1', port=bob.listen_on.port)

        with bob.message_dispatcher.subscribe(Ping) as subscription:
            async with bob.events.wait_new_session() as dial_in_from_alice:
                message = Message(Ping(1234), bob.local_node_id, bob_endpoint)
                await alice.message_dispatcher.send_message(message)

                with trio.fail_after(1):
                    alice_session = await dial_in_from_alice
                assert alice_session.remote_node_id == alice.local_node_id

            ping_msg = await subscription.receive()
            assert isinstance(ping_msg, Message)
            payload = ping_msg.payload
            assert isinstance(payload, Ping)
            assert payload.request_id == 1234

        with alice.message_dispatcher.subscribe(Pong) as subscription:
            message = Message(Pong(1234), alice.local_node_id, alice_endpoint)
            await bob.message_dispatcher.send_message(message)

            pong_msg = await subscription.receive()
            assert isinstance(pong_msg, Message)
            payload = pong_msg.payload
            assert isinstance(payload, Pong)
            assert payload.request_id == 1234


@pytest.mark.trio
async def test_client_request_response():
    alice = ClientFactory()
    bob = ClientFactory()

    logger.info('ALICE: %s', humanize_hash(alice.local_node_id.to_bytes(32, 'big')))
    logger.info('BOB: %s', humanize_hash(bob.local_node_id.to_bytes(32, 'big')))

    async with trio.open_nursery() as nursery:
        async with background_trio_service(bob), background_trio_service(alice):
            await alice.wait_listening()
            await bob.wait_listening()

            alice_endpoint = EndpointFactory(ip_address='127.0.0.1', port=alice.listen_on.port)
            bob_endpoint = EndpointFactory(ip_address='127.0.0.1', port=bob.listen_on.port)

            ready = trio.Event()

            async def pong_when_pinged():
                with bob.message_dispatcher.subscribe(Ping) as subscription:
                    ready.set()
                    request = await subscription.receive()
                    pong = Pong(request.payload.request_id)
                    message = Message(pong, alice.local_node_id, alice_endpoint)
                    await bob.message_dispatcher.send_message(message)

            nursery.start_soon(pong_when_pinged)
            await ready.wait()
            ping_message = Message(Ping(1234), bob.local_node_id, bob_endpoint)
            pong_message = await alice.message_dispatcher.request_response(
                ping_message,
                Pong,
            )

            assert pong_message.node_id == bob.local_node_id
            payload = pong_message.payload
            assert isinstance(payload, Pong)
            assert payload.request_id == 1234
