import math

import pytest
import trio

from alexandria.abc import Node
from alexandria.constants import NODES_PER_PAYLOAD
from alexandria.payloads import Locate, Locations
from alexandria.tools.factories import NodeFactory


@pytest.mark.trio
async def test_client_locate_request_single_response(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        single_location = NodeFactory()

        with bob.message_dispatcher.subscribe(Locate) as locate_subscription:
            async def _handle_locate():
                with trio.fail_after(1):
                    message = await locate_subscription.receive()

                assert isinstance(message.payload, Locate)
                await bob.send_locations(
                    message.node,
                    request_id=message.payload.request_id,
                    locations=(single_location,),
                )

            nursery.start_soon(_handle_locate)

            with trio.fail_after(1):
                messages = await alice.locate(bob.local_node, key=b'key')

            assert len(messages) == 1
            message = messages[0]

            assert isinstance(message.payload, Locations)
            payload = message.payload
            assert payload.total == 1
            assert len(payload.nodes) == 1
            node = Node.from_payload(payload.nodes[0])
            assert node == single_location


@pytest.mark.trio
async def test_client_locate_request_multi_response(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with trio.open_nursery() as nursery:
        locations = NodeFactory.create_batch(100)
        num_expected_messages = int(math.ceil(100 / NODES_PER_PAYLOAD))

        with bob.message_dispatcher.subscribe(Locate) as locate_subscription:
            async def _handle_locate():
                with trio.fail_after(1):
                    message = await locate_subscription.receive()

                assert isinstance(message.payload, Locate)
                await bob.send_locations(
                    message.node,
                    request_id=message.payload.request_id,
                    locations=locations,
                )

            nursery.start_soon(_handle_locate)

            with trio.fail_after(1):
                messages = await alice.locate(bob.local_node, key=b'key')

            assert len(messages) == num_expected_messages
            for message in messages:
                assert isinstance(message.payload, Locations)
                assert message.payload.total == num_expected_messages
