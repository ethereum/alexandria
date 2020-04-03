import math

import pytest
import trio

from alexandria._utils import public_key_to_node_id
from alexandria.constants import NODES_PER_PAYLOAD
from alexandria.payloads import FindNodes, FoundNodes
from alexandria.tools.factories import NodeFactory, PublicKeyFactory


@pytest.mark.trio
async def test_client_find_nodes_request_single_response(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    found_node_id = public_key_to_node_id(PublicKeyFactory())

    async with trio.open_nursery() as nursery:
        with bob.message_dispatcher.subscribe(FindNodes) as find_nodes_subscription:
            async def _handle_find_nodes():
                found_nodes = (NodeFactory(node_id=found_node_id),)

                with trio.fail_after(1):
                    request = await find_nodes_subscription.receive()

                assert isinstance(request.payload, FindNodes)
                await bob.send_found_nodes(
                    request.node,
                    request_id=request.payload.request_id,
                    found_nodes=found_nodes,
                )

            nursery.start_soon(_handle_find_nodes)

            with trio.fail_after(1):
                messages = await alice.find_nodes(bob.local_node, distance=1)

            assert len(messages) == 1
            message = messages[0]

            assert isinstance(message.payload, FoundNodes)
            payload = message.payload
            assert payload.total == 1
            assert len(payload.nodes) == 1
            node = payload.nodes[0]
            assert node[0] == found_node_id


@pytest.mark.trio
async def test_client_find_nodes_request_multi_response(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    found_nodes = NodeFactory.create_batch(100)
    num_expected_messages = int(math.ceil(100 / NODES_PER_PAYLOAD))

    async with trio.open_nursery() as nursery:
        with bob.message_dispatcher.subscribe(FindNodes) as find_nodes_subscription:
            async def _handle_find_nodes():
                with trio.fail_after(1):
                    request = await find_nodes_subscription.receive()

                assert isinstance(request.payload, FindNodes)
                await bob.send_found_nodes(
                    request.node,
                    request_id=request.payload.request_id,
                    found_nodes=found_nodes,
                )

            nursery.start_soon(_handle_find_nodes)

            with trio.fail_after(1):
                messages = await alice.find_nodes(bob.local_node, distance=1)

            assert len(messages) == num_expected_messages
            for message in messages:
                assert isinstance(message.payload, FoundNodes)
                assert message.payload.total == num_expected_messages
