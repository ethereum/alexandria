import logging
import pytest

import trio
from eth_utils import humanize_hash

from async_service import background_trio_service

from alexandria.messages import Ping
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

        with bob.subscribe(Ping) as subscription:
            async with bob.events.wait_new_session() as dial_in_from_alice:
                await alice.ping(1234, bob.local_node_id, bob_endpoint)
                with trio.fail_after(20):
                    alice_session = await dial_in_from_alice
                assert alice_session.remote_node_id == alice.local_node_id

            msg = await subscription.receive()
            assert isinstance(msg, Ping)
            assert msg.id == 1234
