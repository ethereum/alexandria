import pytest
import trio

from alexandria.payloads import Ping, Pong, FindNodes, FoundNodes


@pytest.mark.trio
async def test_client_send_ping(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    with bob.message_dispatcher.subscribe(Ping) as subscription:
        request_id = await alice.send_ping(bob.local_node)

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, Ping)
        assert payload.request_id == request_id


@pytest.mark.trio
async def test_client_send_pong(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    with bob.message_dispatcher.subscribe(Pong) as subscription:
        await alice.send_pong(bob.local_node, request_id=1234)

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, Pong)


@pytest.mark.trio
async def test_client_send_find_nodes(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    with bob.message_dispatcher.subscribe(FindNodes) as subscription:
        request_id = await alice.send_find_nodes(bob.local_node, distance=1)

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, FindNodes)
        assert payload.request_id == request_id


@pytest.mark.trio
async def test_client_send_found_nodes(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    with bob.message_dispatcher.subscribe(FoundNodes) as subscription:
        total_messages = await alice.send_found_nodes(
            bob.local_node,
            request_id=1234,
            found_nodes=(),
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, FoundNodes)
        assert payload.total == total_messages
