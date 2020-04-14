import pytest
import trio

from alexandria.payloads import GraphLinkNodes, GraphLinked


@pytest.mark.trio
async def test_client_link_graph_nodes(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        async with bob.message_dispatcher.subscribe(GraphLinkNodes) as subscription:
            async def _handle_request():
                with trio.fail_after(1):
                    request = await subscription.receive()

                assert isinstance(request.payload, GraphLinkNodes)
                await bob.send_graph_linked(
                    request.node,
                    request_id=request.payload.request_id,
                )

            nursery.start_soon(_handle_request)

            with trio.fail_after(1):
                message = await alice.link_graph_nodes(
                    bob.local_node,
                    left=5,
                    right=7,
                    level=0,
                )

            assert isinstance(message.payload, GraphLinked)


@pytest.mark.trio
async def test_client_link_graph_nodes_left_null(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        async with bob.message_dispatcher.subscribe(GraphLinkNodes) as subscription:
            async def _handle_request():
                with trio.fail_after(1):
                    request = await subscription.receive()

                assert isinstance(request.payload, GraphLinkNodes)
                await bob.send_graph_linked(
                    request.node,
                    request_id=request.payload.request_id,
                )

            nursery.start_soon(_handle_request)

            with trio.fail_after(1):
                message = await alice.link_graph_nodes(
                    bob.local_node,
                    left=None,
                    right=7,
                    level=0,
                )

            assert isinstance(message.payload, GraphLinked)


@pytest.mark.trio
async def test_client_link_graph_nodes_right_null(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        async with bob.message_dispatcher.subscribe(GraphLinkNodes) as subscription:
            async def _handle_request():
                with trio.fail_after(1):
                    request = await subscription.receive()

                assert isinstance(request.payload, GraphLinkNodes)
                await bob.send_graph_linked(
                    request.node,
                    request_id=request.payload.request_id,
                )

            nursery.start_soon(_handle_request)

            with trio.fail_after(1):
                message = await alice.link_graph_nodes(
                    bob.local_node,
                    left=5,
                    right=None,
                    level=0,
                )

            assert isinstance(message.payload, GraphLinked)
