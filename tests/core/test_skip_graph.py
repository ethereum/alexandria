import pytest

from alexandria.tools.skip_graph import validate_graph
from alexandria.skip_graph import SGNode, Graph, NotFound


@pytest.mark.trio
async def test_insert_far_right():
    anchor = SGNode(0)
    graph = Graph(anchor)

    node = await graph.insert(1, anchor)
    assert node.key == 1
    assert node.get_left_neighbor(0) == anchor.key
    assert node.get_right_neighbor(0) is None

    assert node.get_left_neighbor(node.max_level) is None
    assert node.get_right_neighbor(node.max_level) is None

    validate_graph(graph)


@pytest.mark.trio
async def test_insert_sequential_to_the_right_in_order():
    anchor = SGNode(0)
    graph = Graph(anchor)

    node_1, node_2, node_3 = tuple([
        await graph.insert(key, anchor) for key in (1, 2, 3)
    ])

    validate_graph(graph)

    assert anchor.get_left_neighbor(0) is None
    assert anchor.get_right_neighbor(0) == 1

    assert node_1.get_left_neighbor(0) == 0
    assert node_1.get_right_neighbor(0) == 2

    assert node_2.get_left_neighbor(0) == 1
    assert node_2.get_right_neighbor(0) == 3

    assert node_3.get_left_neighbor(0) == 2
    assert node_3.get_right_neighbor(0) is None


@pytest.mark.trio
async def test_insert_sequential_to_the_right_mixed_order():
    anchor = SGNode(0)
    graph = Graph(anchor)

    node_3, node_1, node_2 = tuple([
        await graph.insert(key, anchor) for key in (3, 1, 2)
    ])

    validate_graph(graph)

    assert anchor.get_left_neighbor(0) is None
    assert anchor.get_right_neighbor(0) == 1

    assert node_1.get_left_neighbor(0) == 0
    assert node_1.get_right_neighbor(0) == 2

    assert node_2.get_left_neighbor(0) == 1
    assert node_2.get_right_neighbor(0) == 3

    assert node_3.get_left_neighbor(0) == 2
    assert node_3.get_right_neighbor(0) is None


@pytest.mark.trio
async def test_insert_far_left():
    anchor = SGNode(1)
    graph = Graph(anchor)

    node = await graph.insert(0, anchor)
    assert node.key == 0
    assert node.get_right_neighbor(0) == anchor.key
    assert node.get_left_neighbor(0) is None

    validate_graph(graph)


#
# Search
#
@pytest.mark.trio
async def test_search():
    anchor = SGNode(0)
    graph = Graph(anchor)

    for key in range(5, 100, 5):
        result = await graph.insert(key, anchor)
        assert result.key == key

    validate_graph(graph)

    assert (await graph.search(0, anchor)).key == 0
    node_5 = await graph.search(5, anchor)
    assert node_5.key == 5

    with pytest.raises(NotFound):
        await graph.search(6, anchor)
    with pytest.raises(NotFound):
        await graph.search(6, node_5)
    with pytest.raises(NotFound):
        await graph.search(4, node_5)

    node_80 = await graph.search(80, node_5)
    assert node_80.key == 80


#
# Delete
#
@pytest.mark.parametrize(
    'key_order',
    (
        (7, 6, 5),
        (5, 6, 7),
        (3, 2, 1),
        (1, 2, 3),
        (3, 5, 7, 1, 2, 6),
    ),
)
@pytest.mark.trio
async def test_delete(key_order):
    anchor = SGNode(4)
    graph = Graph(anchor)

    for key in sorted(key_order):
        await graph.insert(key, anchor)

    validate_graph(graph)

    for key in key_order:
        await graph.search(key, anchor)
        await graph.delete(key, anchor)
        with pytest.raises(NotFound):
            await graph.search(key, anchor)

        validate_graph(graph)
