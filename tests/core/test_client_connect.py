import logging
import pytest

import trio
from eth_utils import humanize_hash

from async_service import background_trio_service

from alexandria.tools.factories import ClientFactory, EndpointFactory

logger = logging.getLogger('alexandria.testing')


@pytest.mark.trio
async def test_client_connect(nursery):
    alice = ClientFactory()
    bob = ClientFactory()

    logger.info('ALICE: %s', humanize_hash(alice.local_node_id.to_bytes(32, 'big')))
    logger.info('BOB: %s', humanize_hash(bob.local_node_id.to_bytes(32, 'big')))

    async with background_trio_service(bob), background_trio_service(alice):
        await alice.wait_listening()
        await bob.wait_listening()

        bob_endpoint = EndpointFactory(ip_address='127.0.0.1', port=bob.listen_on.port)

        async with bob.events.wait_new_connection() as new_connection:
            await alice.ping(bob.local_node_id, bob_endpoint)
            with trio.fail_after(1):
                bob_connection = await new_connection
            assert bob_connection.session.remote_node_id == bob.local_node_id
