import pytest
import trio

from alexandria.payloads import GraphInsert, GraphInserted


@pytest.mark.trio
async def test_client_graph_insert(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        async with bob.message_dispatcher.subscribe(GraphInsert) as subscription:
            async def _handle_request():
                with trio.fail_after(1):
                    request = await subscription.receive()

                assert isinstance(request.payload, GraphInsert)
                await bob.send_graph_inserted(
                    request.node,
                    request_id=request.payload.request_id,
                )

            nursery.start_soon(_handle_request)

            with trio.fail_after(1):
                message = await alice.graph_insert(
                    bob.local_node,
                    key=1234,
                )

            assert isinstance(message.payload, GraphInserted)
