import pytest
import trio

from alexandria.payloads import GraphDelete, GraphDeleted


@pytest.mark.trio
async def test_client_graph_delete(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        async with bob.message_dispatcher.subscribe(GraphDelete) as subscription:
            async def _handle_request():
                with trio.fail_after(1):
                    request = await subscription.receive()

                assert isinstance(request.payload, GraphDelete)
                await bob.send_graph_deleted(
                    request.node,
                    request_id=request.payload.request_id,
                )

            nursery.start_soon(_handle_request)

            with trio.fail_after(1):
                message = await alice.graph_delete(
                    bob.local_node,
                    key=1234,
                )

            assert isinstance(message.payload, GraphDeleted)
