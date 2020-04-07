import time

import pytest
import trio

from alexandria.constants import SESSION_IDLE_TIMEOUT


@pytest.mark.trio
async def test_client_detects_idle_sessions(alice_and_bob_clients, autojump_clock):
    alice, bob = alice_and_bob_clients

    got_completed_handshake = alice.events.handshake_complete.subscribe()

    async with got_completed_handshake:
        await alice.send_ping(bob.local_node)

        bob_session = await got_completed_handshake

        await trio.sleep(0.01)

        # now cancel bob's session so that he stops pinging.
        bob.get_manager().cancel()

        async with alice.events.session_idle.subscribe() as session_went_idle:
            bob_session.last_message_at = time.monotonic() - SESSION_IDLE_TIMEOUT

            with trio.fail_after(SESSION_IDLE_TIMEOUT * 2):
                idle_session = await session_went_idle

            assert idle_session.remote_node_id == bob.local_node_id
