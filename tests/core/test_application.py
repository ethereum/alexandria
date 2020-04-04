import logging
import random

from async_service import background_trio_service
from async_exit_stack import AsyncExitStack
import pytest
import trio

from alexandria._utils import humanize_node_id
from alexandria.abc import Node
from alexandria.kademlia import KademliaConfig
from alexandria.tools.factories import ApplicationFactory


logger = logging.getLogger('alexandria.testing')


@pytest.fixture
async def bootnode():
    bootnode = ApplicationFactory()
    logger.info('BOOTNODE: %s', humanize_node_id(bootnode.client.local_node_id))
    async with bootnode.client.events.listening.subscribe() as listening:
        async with background_trio_service(bootnode):
            await listening
            yield bootnode


@pytest.mark.skip('integration testing')
@pytest.mark.trio
async def test_application(bootnode):
    bootnodes = (Node(bootnode.client.local_node_id, bootnode.client.listen_on),)

    new_connections = bootnode.client.events.handshake_complete.subscribe()
    connected_nodes = []

    async def monitor_bootnode():
        async with new_connections:
            async for session in new_connections:
                logger.info('NODE_CONNECTED_TO_BOOTNODE: %s', humanize_node_id(session.remote_node_id))  # noqa: E501
                connected_nodes.append(session.remote_node_id)

    config = KademliaConfig(
        LOOKUP_INTERVAL=10,
        ANNOUNCE_INTERVAL=30,
    )
    async with AsyncExitStack() as stack:
        for i in range(12):
            await trio.sleep(random.random())
            app = ApplicationFactory(bootnodes=bootnodes, config=config)
            logger.info('CLIENT-%d: %s', i, humanize_node_id(app.client.local_node_id))
            await stack.enter_async_context(background_trio_service(app))
        await trio.sleep_forever()
