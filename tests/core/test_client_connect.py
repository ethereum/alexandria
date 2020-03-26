import pytest

from async_service import background_trio_service

from alexandria.client import Node
from alexandria.tools.factories import ClientFactory, NodeFactory


@pytest.mark.trio
async def test_client_connect(nursery):
    alice = ClientFactory()
    bob = ClientFactory()

    bob_endpoint = NodeFactory()

    bob_node = Node(bob.private_key, bob_endpoint)

    async with background_trio_service(bob), background_trio_service(alice):
        await alice.wait_listening()
        await bob.wait_listening()

        await alice.handshake(bob_node)
