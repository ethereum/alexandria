import logging
import random

from async_service import background_trio_service
from async_exit_stack import AsyncExitStack
from eth_utils import to_dict
import pytest
import trio

from alexandria._utils import humanize_node_id
from alexandria.abc import Node
from alexandria.content_manager import StorageConfig
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


@to_dict
def make_local_content():
    for _ in range(256):
        content_id = random.randint(0, 65535)
        key = b'key-%d' % content_id
        value = b'value-%d' % content_id
        yield key, value


#@pytest.mark.skip('integration testing')
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
        # LOOKUP_INTERVAL=10,
        ANNOUNCE_INTERVAL=30,
        ANNOUNCE_CONCURRENCY=1,
        storage_config=StorageConfig(
            ephemeral_storage_size=1024,
            ephemeral_index_size=500,
            cache_storage_size=1024,
            cache_index_size=100
        ),
    )
    async with AsyncExitStack() as stack:
        for i in range(12):
            # small delay between starting each client
            await trio.sleep(random.random())
            # content database
            local_content = make_local_content()
            app = ApplicationFactory(bootnodes=bootnodes, local_content=local_content, config=config)
            logger.info('CLIENT-%d: %s', i, humanize_node_id(app.client.local_node_id))
            await stack.enter_async_context(background_trio_service(app))
        await trio.sleep_forever()
