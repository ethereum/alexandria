import pytest
import trio

from alexandria.payloads import Ping, Pong


@pytest.mark.trio
async def test_client_ping_request(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        async with bob.message_dispatcher.subscribe(Ping) as ping_subscription:
            async def _handle_ping():
                with trio.fail_after(1):
                    ping = await ping_subscription.receive()

                assert isinstance(ping.payload, Ping)
                await bob.send_pong(ping.node, request_id=ping.payload.request_id)

            nursery.start_soon(_handle_ping)

            with trio.fail_after(1):
                message = await alice.ping(bob.local_node)

            assert isinstance(message.payload, Pong)
