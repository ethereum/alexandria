import pytest

import trio

from async_service import background_trio_service

from alexandria.messages import Ping, Message, Pong
from alexandria.tools import driver
from alexandria.tools.factories import ClientFactory, EndpointFactory


@pytest.mark.skip(reason='Driver API not implemented yet')
@pytest.mark.trio
async def test_client_connect(nursery):
    alice_endpoint = EndpointFactory(ip_address='127.0.0.1')
    alice = driver.driver(
        driver.listen(alice_endpoint),
        driver.send_message(Ping(1234)),
        driver.wait_for_message(Pong),
    )
    bob = ClientFactory()

    nursery.start_soon(alice.drive)

    async with background_trio_service(bob), background_trio_service(alice):
        await alice.wait_listening()
        await bob.wait_listening()

        bob_endpoint = EndpointFactory(ip_address='127.0.0.1', port=bob.listen_on.port)

        with bob.subscribe(Ping) as subscription:
            async with bob.events.wait_new_session() as dial_in_from_alice:
                await alice.ping(1234, bob.local_node_id, bob_endpoint)
                with trio.fail_after(20):
                    alice_session = await dial_in_from_alice
                assert alice_session.remote_node_id == alice.local_node_id

            ping_msg = await subscription.receive()
            assert isinstance(ping_msg, Message)
            payload = ping_msg.payload
            assert isinstance(payload, Ping)
            assert payload.id == 1234
