import logging
import pathlib
import random
import tempfile

from async_service import background_trio_service
from async_exit_stack import AsyncExitStack
import pytest
import trio

from alexandria._utils import humanize_node_id
from alexandria.abc import Node
from alexandria.content_manager import StorageConfig
from alexandria.durable_db import DurableDB
from alexandria.kademlia import KademliaConfig
from alexandria.tools.factories import ApplicationFactory


logger = logging.getLogger('alexandria.testing')


@pytest.fixture
def base_db_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir)


@pytest.fixture
async def bootnode(base_db_path):
    config = KademliaConfig(can_initialize_network_skip_graph=True)
    durable_db = make_durable_db(base_db_path / f"bootnode")

    bootnode = ApplicationFactory(
        durable_db=durable_db,
        config=config,
    )
    logger.info('BOOTNODE: %s', humanize_node_id(bootnode.client.local_node_id))
    async with bootnode.client.events.listening.subscribe() as listening:
        async with background_trio_service(bootnode):
            await listening.receive()
            yield bootnode


def make_durable_db(db_path):
    db_path.mkdir(parents=True, exist_ok=True)
    db = DurableDB(db_path)

    for _ in range(32):
        content_id = random.randint(0, 65535)
        key = b'k%d' % content_id
        value = b'v%d' % content_id
        db.set(key, value)

    return db


#@pytest.mark.skip('integration testing')
@pytest.mark.trio
async def test_application(bootnode, base_db_path):
    bootnodes = (Node(bootnode.client.local_node_id, bootnode.client.listen_on),)

    connected_nodes = []

    async def monitor_bootnode():
        async with bootnode.client.events.handshake_complete.subscribe() as subscription:
            async for session in subscription:
                logger.info('NODE_CONNECTED_TO_BOOTNODE: %s', humanize_node_id(session.remote_node_id))  # noqa: E501
                connected_nodes.append(session.remote_node_id)

    config = KademliaConfig(
        LOOKUP_INTERVAL=20,
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
        for i in range(4):
            # small delay between starting each client.
            await trio.sleep(random.random())
            # content database
            durable_db = make_durable_db(base_db_path / f"client-{i}")
            app = ApplicationFactory(
                bootnodes=bootnodes,
                durable_db=durable_db,
                config=config,
            )
            logger.info('CLIENT-%d: %s', i, humanize_node_id(app.client.local_node_id))
            await stack.enter_async_context(background_trio_service(app))
        await trio.sleep_forever()
