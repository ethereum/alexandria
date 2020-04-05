import pytest
import trio

from alexandria.payloads import Advertise, Ack


@pytest.mark.trio
async def test_client_advertise_request(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        with bob.message_dispatcher.subscribe(Advertise) as advertise_subscription:
            async def _handle_advertise():
                with trio.fail_after(1):
                    message = await advertise_subscription.receive()

                assert isinstance(message.payload, Advertise)
                await bob.send_ack(message.node, request_id=message.payload.request_id)

            nursery.start_soon(_handle_advertise)

            with trio.fail_after(1):
                message = await alice.advertise(bob.local_node, key=b'key', who=bob.local_node)

            assert isinstance(message.payload, Ack)
