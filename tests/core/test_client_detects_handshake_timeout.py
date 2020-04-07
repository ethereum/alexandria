import pytest
from async_service import background_trio_service
import trio

from alexandria.constants import HANDSHAKE_TIMEOUT
from alexandria.tools.factories import ClientFactory


@pytest.mark.trio
async def test_client_detects_handshake_timeout(alice_and_bob_clients, autojump_clock):
    alice = ClientFactory()
    bob = ClientFactory()

    async with background_trio_service(alice):
        got_handshake_timeout = bob.events.handshake_timeout.subscribe()

        async with got_handshake_timeout:
            await alice.send_ping(bob.local_node)
            await trio.sleep(0.01)

            assert alice.pool.has_session(bob.local_node_id)

            with pytest.raises(trio.TooSlowError):
                with trio.fail_after(HANDSHAKE_TIMEOUT):
                    await got_handshake_timeout

            assert not alice.pool.has_session(bob.local_node_id)
