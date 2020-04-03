import logging

import pytest

from async_service import background_trio_service

from alexandria.tools.factories import ClientFactory

logger = logging.getLogger('alexandria.testing')


@pytest.fixture
async def alice_and_bob_clients():
    alice = ClientFactory()
    bob = ClientFactory()

    logger.info('ALICE: %s', alice.local_node)
    logger.info('BOB: %s', bob.local_node)

    alice_listening = alice.events.listening.subscribe()
    bob_listening = bob.events.listening.subscribe()
    async with alice_listening, bob_listening:
        async with background_trio_service(bob), background_trio_service(alice):
            await alice_listening
            await bob_listening

            yield alice, bob
