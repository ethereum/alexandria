import pathlib
import tempfile

from async_service import background_trio_service
import pytest
import trio

from alexandria.abc import ContentBundle
from alexandria.rpc import RPCServer
from alexandria.payloads import Advertise, FindNodes, Locate, Ping, Retrieve
from alexandria.tools.factories import ApplicationFactory
from alexandria.w3 import get_w3


@pytest.fixture
def ipc_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir) / 'jsonrpc.ipc'


@pytest.fixture
async def peers(ipc_path):
    bootnodes = []
    peer_a = ApplicationFactory()
    async with background_trio_service(peer_a):
        bootnodes.append(peer_a.client.local_node)
        peer_b = ApplicationFactory(bootnodes=bootnodes)
        async with background_trio_service(peer_b):
            bootnodes.append(peer_b.client.local_node)
            peer_c = ApplicationFactory()
            async with background_trio_service(peer_c):
                bootnodes.append(peer_c.client.local_node)
                peer_d = ApplicationFactory(bootnodes=bootnodes)
                async with background_trio_service(peer_d):
                    bootnodes.append(peer_d.client.local_node)
                    peer_e = ApplicationFactory(bootnodes=bootnodes)
                    async with background_trio_service(peer_e):
                        yield (peer_a, peer_b, peer_c, peer_d, peer_e)


@pytest.fixture
async def rpc_node(ipc_path, peers):
    bootnodes = tuple(
        peer.client.local_node
        for peer in peers
    )
    alice = ApplicationFactory(bootnodes=bootnodes)
    async with background_trio_service(alice):
        json_rpc_server = RPCServer(
            ipc_path=ipc_path,
            client=alice.client,
            network=alice.network,
            kademlia=alice.kademlia,
            routing_table=alice.routing_table,
        )
        async with background_trio_service(json_rpc_server):
            await json_rpc_server.wait_serving()
            for _ in range(32):
                await trio.hazmat.checkpoint()
            yield alice


@pytest.fixture
async def w3(ipc_path, rpc_node):
    await trio.sleep(1)
    return get_w3(ipc_path=ipc_path)


@pytest.mark.trio
async def test_rpc_get_local_node(w3, rpc_node):
    node = await trio.to_thread.run_sync(w3.alexandria.get_local_node_id)
    assert node == rpc_node.client.local_node


@pytest.mark.trio
async def test_rpc_get_routing_table_stats(w3, rpc_node):
    stats = await trio.to_thread.run_sync(w3.alexandria.get_routing_table_stats)
    assert stats == rpc_node.routing_table.get_stats()


@pytest.mark.trio
async def test_rpc_get_routing_table_bucket_info(w3, rpc_node):
    info = await trio.to_thread.run_sync(w3.alexandria.get_routing_table_bucket_info, 255)
    assert info == rpc_node.routing_table.get_bucket_info(255)
    assert all(isinstance(value, int) for value in info.nodes)
    assert all(isinstance(value, int) for value in info.replacement_cache)


@pytest.mark.trio
async def test_rpc_ping(w3, rpc_node, peers):
    bob = peers[0]
    async with bob.client.message_dispatcher.subscribe(Ping) as subscription:
        elapsed = await trio.to_thread.run_sync(w3.alexandria.ping, bob.client.local_node)
        with trio.fail_after(1):
            await subscription.receive()
        assert elapsed > 0


@pytest.mark.trio
async def test_rpc_find_nodes(w3, rpc_node, peers):
    bob = peers[0]
    async with bob.client.message_dispatcher.subscribe(FindNodes) as subscription:
        found_nodes = await trio.to_thread.run_sync(
            w3.alexandria.find_nodes,
            bob.client.local_node,
            256,
        )
        with trio.fail_after(1):
            await subscription.receive()
        assert len(found_nodes) == len(bob.routing_table.buckets[255])


@pytest.mark.trio
async def test_rpc_advertise(w3, rpc_node, peers):
    bob = peers[0]

    async with bob.client.message_dispatcher.subscribe(Advertise) as subscription:
        elapsed = await trio.to_thread.run_sync(
            w3.alexandria.advertise,
            bob.client.local_node,
            b'key',
            rpc_node.client.local_node,
        )
        with trio.fail_after(1):
            await subscription.receive()
        assert elapsed > 0


@pytest.mark.trio
async def test_rpc_locate(w3, rpc_node, peers):
    bob = peers[0]
    bob.kademlia.content_manager.ingest_content(ContentBundle(
        b'key-a',
        b'value-a',
        bob.client.local_node_id,
    ))
    async with bob.client.message_dispatcher.subscribe(Locate) as subscription:
        found_nodes = await trio.to_thread.run_sync(
            w3.alexandria.locate,
            bob.client.local_node,
            b'key-a',
        )
        with trio.fail_after(1):
            await subscription.receive()
        assert len(found_nodes) == 1
        node = found_nodes[0]
        assert node.node_id == bob.client.local_node_id


@pytest.mark.trio
async def test_rpc_retrieve(w3, rpc_node, peers):
    bob = peers[0]
    bob.kademlia.content_manager.ingest_content(ContentBundle(
        b'key-a',
        b'value-a',
        bob.client.local_node_id,
    ))
    async with bob.client.message_dispatcher.subscribe(Retrieve) as subscription:
        data = await trio.to_thread.run_sync(
            w3.alexandria.retrieve,
            bob.client.local_node,
            b'key-a',
        )
        with trio.fail_after(1):
            await subscription.receive()
        assert data == b'value-a'


@pytest.mark.trio
async def test_rpc_contentStats(w3, rpc_node):
    stats = await trio.to_thread.run_sync(w3.alexandria.get_content_stats)
    assert stats == rpc_node.kademlia.content_manager.get_stats()


@pytest.mark.trio
async def test_rpc_addContent_durable(w3, rpc_node):
    assert b'key' not in set(rpc_node.kademlia.content_manager.iter_content_keys())
    await trio.to_thread.run_sync(w3.alexandria.add_content, b'key', b'value', False)
    assert b'key' in set(rpc_node.kademlia.content_manager.iter_content_keys())
    assert b'key' in set(rpc_node.kademlia.content_manager.durable_db.keys())
    assert b'key' not in set(rpc_node.kademlia.content_manager.ephemeral_db.keys())
    assert b'key' not in set(rpc_node.kademlia.content_manager.cache_db.keys())


@pytest.mark.trio
async def test_rpc_addContent_ephemeral(w3, rpc_node):
    assert b'key' not in set(rpc_node.kademlia.content_manager.iter_content_keys())
    await trio.to_thread.run_sync(w3.alexandria.add_content, b'key', b'value', True)
    assert b'key' in set(rpc_node.kademlia.content_manager.iter_content_keys())
    assert b'key' not in set(rpc_node.kademlia.content_manager.durable_db.keys())
    assert b'key' in set(rpc_node.kademlia.content_manager.ephemeral_db.keys())
    assert b'key' in set(rpc_node.kademlia.content_manager.cache_db.keys())
