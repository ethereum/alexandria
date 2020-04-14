import pytest
import trio

from alexandria.skip_graph import SGNode
from alexandria.payloads import (
    Advertise, Ack,
    FindNodes, FoundNodes,
    Locate, Locations,
    Ping, Pong,
    Retrieve, Chunk,
    GraphGetIntroduction, GraphIntroduction,
    GraphGetNode, GraphNode,
    GraphLinkNodes, GraphLinked,
)


@pytest.mark.trio
async def test_client_send_ping(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(Ping) as subscription:
        request_id = await alice.send_ping(bob.local_node)

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, Ping)
        assert payload.request_id == request_id


@pytest.mark.trio
async def test_client_send_pong(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(Pong) as subscription:
        await alice.send_pong(bob.local_node, request_id=1234)

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, Pong)


@pytest.mark.trio
async def test_client_send_find_nodes(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(FindNodes) as subscription:
        request_id = await alice.send_find_nodes(bob.local_node, distance=1)

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, FindNodes)
        assert payload.request_id == request_id


@pytest.mark.trio
async def test_client_send_found_nodes(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(FoundNodes) as subscription:
        total_messages = await alice.send_found_nodes(
            bob.local_node,
            request_id=1234,
            found_nodes=(),
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, FoundNodes)
        assert payload.total == total_messages


@pytest.mark.trio
async def test_client_send_advertise(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(Advertise) as subscription:
        request_id = await alice.send_advertise(
            bob.local_node,
            key=b'key',
            who=bob.local_node,
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, Advertise)
        assert payload.request_id == request_id
        assert payload.key == b'key'
        assert payload.node == (bob.local_node_id, bob.listen_on.ip_address.packed, bob.listen_on.port)  # noqa: E501


@pytest.mark.trio
async def test_client_send_ack(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(Ack) as subscription:
        await alice.send_ack(bob.local_node, request_id=1234)

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, Ack)
        assert payload.request_id == 1234


@pytest.mark.trio
async def test_client_send_locate(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(Locate) as subscription:
        request_id = await alice.send_locate(bob.local_node, key=b'key')

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, Locate)
        assert payload.request_id == request_id
        assert payload.key == b'key'


@pytest.mark.trio
async def test_client_send_locations(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(Locations) as subscription:
        total_messages = await alice.send_locations(bob.local_node, request_id=1234, locations=())

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, Locations)
        assert payload.request_id == 1234
        assert payload.total == total_messages
        # This is an odd Hashable type...
        assert tuple(payload.nodes) == ()


@pytest.mark.trio
async def test_client_send_retrieve(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(Retrieve) as subscription:
        request_id = await alice.send_retrieve(bob.local_node, key=b'key')

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, Retrieve)
        assert payload.request_id == request_id
        assert payload.key == b'key'


@pytest.mark.trio
async def test_client_send_chunks(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(Chunk) as subscription:
        total_chunks = await alice.send_chunks(bob.local_node, request_id=1234, data=b'key')

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, Chunk)
        assert payload.request_id == 1234
        assert payload.total == total_chunks
        assert payload.index == 0
        assert payload.data == b'key'


@pytest.mark.trio
async def test_client_send_graph_introduce(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(GraphGetIntroduction) as subscription:
        request_id = await alice.send_graph_get_introduction(bob.local_node)

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, GraphGetIntroduction)
        assert payload.request_id == request_id


@pytest.mark.trio
async def test_client_send_graph_introduction(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(GraphIntroduction) as subscription:
        await alice.send_graph_introduction(
            bob.local_node,
            request_id=1234,
            graph_nodes=(SGNode(5, (3, 2), (7, 9)), SGNode(7, (5,), (9,))),
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, GraphIntroduction)
        assert payload.request_id == 1234
        assert len(payload.nodes) == 2
        node_5, node_7 = payload.nodes[0].to_sg_node(), payload.nodes[1].to_sg_node()

        assert node_5.key == 5
        assert node_5.neighbors_left == [3, 2]
        assert node_5.neighbors_right == [7, 9]

        assert node_7.key == 7
        assert node_7.neighbors_left == [5]
        assert node_7.neighbors_right == [9]


@pytest.mark.trio
async def test_client_send_graph_get_node(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(GraphGetNode) as subscription:
        request_id = await alice.send_graph_get_node(bob.local_node, key=5)

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, GraphGetNode)
        assert payload.request_id == request_id
        assert payload.key == b'\x05'


@pytest.mark.trio
async def test_client_send_graph_node(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(GraphNode) as subscription:
        await alice.send_graph_node(
            bob.local_node,
            request_id=1234,
            sg_node=SGNode(5, (3, 2), (7, 9)),
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, GraphNode)
        assert payload.request_id == 1234

        node = payload.node.to_sg_node()

        assert node.key == 5
        assert node.neighbors_left == [3, 2]
        assert node.neighbors_right == [7, 9]


@pytest.mark.trio
async def test_client_send_graph_node_null(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(GraphNode) as subscription:
        await alice.send_graph_node(
            bob.local_node,
            request_id=1234,
            sg_node=None,
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, GraphNode)
        assert payload.request_id == 1234
        assert payload.node is None


@pytest.mark.trio
async def test_client_send_graph_link_nodes(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(GraphLinkNodes) as subscription:
        request_id = await alice.send_graph_link_nodes(
            bob.local_node,
            left=5,
            right=7,
            level=0,
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, GraphLinkNodes)
        assert payload.request_id == request_id
        assert payload.left == b'\x05'
        assert payload.right == b'\x07'
        assert payload.level == 0


@pytest.mark.trio
async def test_client_send_graph_link_nodes_left_null(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(GraphLinkNodes) as subscription:
        request_id = await alice.send_graph_link_nodes(
            bob.local_node,
            left=None,
            right=7,
            level=0,
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, GraphLinkNodes)
        assert payload.request_id == request_id
        assert payload.left is None
        assert payload.right == b'\x07'
        assert payload.level == 0


@pytest.mark.trio
async def test_client_send_graph_link_nodes_right_null(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(GraphLinkNodes) as subscription:
        request_id = await alice.send_graph_link_nodes(
            bob.local_node,
            left=5,
            right=None,
            level=0,
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, GraphLinkNodes)
        assert payload.request_id == request_id
        assert payload.left == b'\x05'
        assert payload.right is None
        assert payload.level == 0


@pytest.mark.trio
async def test_client_send_graph_linked(alice_and_bob_clients):
    alice, bob = alice_and_bob_clients

    async with bob.message_dispatcher.subscribe(GraphLinked) as subscription:
        await alice.send_graph_linked(
            bob.local_node,
            request_id=1234,
        )

        with trio.fail_after(1):
            message = await subscription.receive()

        assert message.node == alice.local_node
        payload = message.payload
        assert isinstance(payload, GraphLinked)
        assert payload.request_id == 1234
