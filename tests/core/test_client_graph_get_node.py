import pytest
import trio

from alexandria.skip_graph import SGNode
from alexandria.payloads import GraphGetNode, GraphNode


@pytest.mark.trio
async def test_client_get_graph_node(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        async with bob.message_dispatcher.subscribe(GraphGetNode) as subscription:
            async def _handle_request():
                with trio.fail_after(1):
                    request = await subscription.receive()

                assert isinstance(request.payload, GraphGetNode)
                assert request.payload.key == b'\x05'
                await bob.send_graph_node(
                    request.node,
                    request_id=request.payload.request_id,
                    sg_node=SGNode(5, (3, 0), (7, 9)),
                )

            nursery.start_soon(_handle_request)

            with trio.fail_after(1):
                message = await alice.get_graph_node(bob.local_node, key=5)

            assert isinstance(message.payload, GraphNode)

            node = message.payload.node.to_sg_node()
            assert node.key == 5
            assert node.neighbors[0] == [3, 0]
            assert node.neighbors[1] == [7, 9]


@pytest.mark.trio
async def test_client_get_graph_node_null(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        async with bob.message_dispatcher.subscribe(GraphGetNode) as subscription:
            async def _handle_request():
                with trio.fail_after(1):
                    request = await subscription.receive()

                assert isinstance(request.payload, GraphGetNode)
                assert request.payload.key == b'\x05'
                await bob.send_graph_node(
                    request.node,
                    request_id=request.payload.request_id,
                    sg_node=None,
                )

            nursery.start_soon(_handle_request)

            with trio.fail_after(1):
                message = await alice.get_graph_node(bob.local_node, key=5)

            assert isinstance(message.payload, GraphNode)
            assert message.payload.node is None
