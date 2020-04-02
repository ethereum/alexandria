import logging

from async_service import background_trio_service
from async_exit_stack import AsyncExitStack
import pytest
import trio

from alexandria._utils import humanize_node_id
from alexandria.abc import Node
from alexandria.tools.factories import ApplicationFactory


logger = logging.getLogger('alexandria.testing')


@pytest.fixture
async def bootnode():
    bootnode = ApplicationFactory()
    logger.info('BOOTNODE: %s', humanize_node_id(bootnode.client.local_node_id))
    async with bootnode.client.events.listening.subscribe() as listening:
        logger.error('HERE.0')
        async with background_trio_service(bootnode):
            logger.error('HERE.1')
            await listening
            logger.error('HERE.2')
            yield bootnode


@pytest.mark.trio
async def test_application(bootnode):
    bootnodes = (Node(bootnode.client.local_node_id, bootnode.client.listen_on),)

    new_connections = bootnode.client.events.handshake_complete.subscribe()
    connected_nodes = []

    async def monitor_bootnode():
        async with new_connections:
            async for session in new_connections:
                connected_nodes.append(session.remote_node_id)

    async with AsyncExitStack() as stack:
        for i in range(2):
            app = ApplicationFactory(bootnodes=bootnodes)
            logger.info('CLIENT-%d: %s', i, humanize_node_id(app.client.local_node_id))
            await stack.enter_async_context(background_trio_service(app))
        await trio.sleep(60)
