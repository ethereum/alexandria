import pytest

import trio


@pytest.mark.trio
async def test_client_inbound_connect(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    got_dial_in = bob.events.new_session.subscribe()
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

    got_dial_in = alice.events.new_session.subscribe()
    got_completed_handshake = alice.events.handshake_complete.subscribe()

    async with got_dial_in, got_completed_handshake:
        await alice.send_ping(bob.local_node)

        with trio.fail_after(1):
            bob_session_from_dial_in = await got_dial_in
        assert bob_session_from_dial_in.remote_node_id == bob.local_node_id

        with trio.fail_after(1):
            bob_session_from_complete_handhshake = await got_completed_handshake
        assert bob_session_from_complete_handhshake.remote_node_id == bob.local_node_id
