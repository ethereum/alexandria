import pytest
import trio

from alexandria.skip_graph import SGNode
from alexandria.payloads import GraphGetIntroduction, GraphIntroduction


@pytest.mark.trio
async def test_client_get_graph_introduction_request(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        async with bob.message_dispatcher.subscribe(GraphGetIntroduction) as subscription:
            async def _handle_introduction():
                with trio.fail_after(1):
                    request = await subscription.receive()

                assert isinstance(request.payload, GraphGetIntroduction)
                await bob.send_graph_introduction(
                    request.node,
                    request_id=request.payload.request_id,
                    graph_nodes=(SGNode(5, (3, 0), (7, 9)),),
                )

            nursery.start_soon(_handle_introduction)

            with trio.fail_after(1):
                message = await alice.get_graph_introduction(bob.local_node)

            assert isinstance(message.payload, GraphIntroduction)
