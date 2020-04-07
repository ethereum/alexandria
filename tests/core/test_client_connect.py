import logging

import pytest

from async_service import background_trio_service
import trio

from alexandria.tools.factories import ClientFactory

logger = logging.getLogger('alexandria.testing')


@pytest.mark.trio
async def test_client_inbound_connect(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    got_dial_in = bob.events.session_created.subscribe()
    got_completed_handshake = bob.events.handshake_complete.subscribe()

    async with got_dial_in, got_completed_handshake:
        await alice.send_ping(bob.local_node)

        with trio.fail_after(1):
            alice_session_from_dial_in = await got_dial_in
        assert alice_session_from_dial_in.remote_node_id == alice.local_node_id

        with trio.fail_after(1):
            alice_session_from_complete_handhshake = await got_completed_handshake
        assert alice_session_from_complete_handhshake.remote_node_id == alice.local_node_id


@pytest.mark.trio
async def test_client_outbound_connect(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    got_dial_in = alice.events.session_created.subscribe()
    got_completed_handshake = alice.events.handshake_complete.subscribe()

    async with got_dial_in, got_completed_handshake:
        await alice.send_ping(bob.local_node)

        with trio.fail_after(1):
            bob_session_from_dial_in = await got_dial_in
        assert bob_session_from_dial_in.remote_node_id == bob.local_node_id

        with trio.fail_after(1):
            bob_session_from_complete_handhshake = await got_completed_handshake
        assert bob_session_from_complete_handhshake.remote_node_id == bob.local_node_id


@pytest.mark.trio
async def test_client_symetric_connect():
    alice = ClientFactory()
    bob = ClientFactory()

    logger.info('ALICE: %s', alice.local_node)
    logger.info('BOB: %s', bob.local_node)

    async with background_trio_service(alice):
        # this triggers the creation of an outbound session but the packet
        # never gets to bob because his client isn't listening yet.
        await alice.send_ping(bob.local_node)

        # let the event loop tick for a few ticks to ensure that the packet has
        # been sent.
        for _ in range(20):
            await trio.hazmat.checkpoint()

        alice_handshake_complete = alice.events.handshake_complete.subscribe()
        bob_handshake_complete = bob.events.handshake_complete.subscribe()
        async with background_trio_service(bob):
            await bob.send_ping(alice.local_node)

            with trio.fail_after(1):
                bob_session = await alice_handshake_complete
                alice_session = await bob_handshake_complete

            assert not bob_session.is_initiator
            assert alice_session.is_initiator
