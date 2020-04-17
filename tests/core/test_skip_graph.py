import pytest

from alexandria.tools.skip_graph import validate_graph
from alexandria.skip_graph import SGNode, LocalGraph, NotFound, LEFT, RIGHT


@pytest.mark.trio
async def test_insert_far_right():
    anchor = SGNode(0)
    graph = LocalGraph(anchor)

    node = await graph.insert(1)
    assert node.key == 1
    assert node.get_neighbor(0, LEFT) == anchor.key
    assert node.get_neighbor(0, RIGHT) is None

    assert node.get_neighbor(node.max_level, LEFT) is None
    assert node.get_neighbor(node.max_level, RIGHT) is None

    validate_graph(graph)


@pytest.mark.trio
async def test_insert_sequential_to_the_correct_in_order():
    anchor = SGNode(0)
    graph = LocalGraph(anchor)

    node_1, node_2, node_3 = tuple([
        await graph.insert(key) for key in (1, 2, 3)
    ])

    validate_graph(graph)

    assert anchor.get_neighbor(0, LEFT) is None
    assert anchor.get_neighbor(0, RIGHT) == 1

    assert node_1.get_neighbor(0, LEFT) == 0
    assert node_1.get_neighbor(0, RIGHT) == 2

    assert node_2.get_neighbor(0, LEFT) == 1
    assert node_2.get_neighbor(0, RIGHT) == 3

    assert node_3.get_neighbor(0, LEFT) == 2
    assert node_3.get_neighbor(0, RIGHT) is None


@pytest.mark.trio
async def test_insert_sequential_to_the_correct_mixed_order():
    anchor = SGNode(0)
    graph = LocalGraph(anchor)

    node_3, node_1, node_2 = tuple([
        await graph.insert(key) for key in (3, 1, 2)
    ])

    validate_graph(graph)

    assert anchor.get_neighbor(0, LEFT) is None
    assert anchor.get_neighbor(0, RIGHT) == 1

    assert node_1.get_neighbor(0, LEFT) == 0
    assert node_1.get_neighbor(0, RIGHT) == 2

    assert node_2.get_neighbor(0, LEFT) == 1
    assert node_2.get_neighbor(0, RIGHT) == 3

    assert node_3.get_neighbor(0, LEFT) == 2
    assert node_3.get_neighbor(0, RIGHT) is None


@pytest.mark.trio
async def test_insert_far_left():
    anchor = SGNode(1)
    graph = LocalGraph(anchor)

    node = await graph.insert(0)
    assert node.key == 0
    assert node.get_neighbor(0, RIGHT) == anchor.key
    assert node.get_neighbor(0, LEFT) is None

    validate_graph(graph)


#
# Search
#
@pytest.mark.trio
async def test_search():
    anchor = SGNode(0)
    graph = LocalGraph(anchor)

    for key in range(5, 100, 5):
        result = await graph.insert(key)
        assert result.key == key

    validate_graph(graph)

    assert (await graph.search(0)).key == 0
    node_5 = await graph.search(5)
    assert node_5.key == 5

    with pytest.raises(NotFound):
        await graph.search(6)

    graph.cursor = node_5
    with pytest.raises(NotFound):
        await graph.search(6)
    with pytest.raises(NotFound):
        await graph.search(4)

    node_80 = await graph.search(80)
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
    graph = LocalGraph(anchor)

    for key in sorted(key_order):
        await graph.insert(key)

    validate_graph(graph)

    assert all(key in graph.db._db for key in key_order)
    for key in key_order:
        await graph.search(key)
        await graph.delete(key)
        with pytest.raises(NotFound):
            await graph.search(key)

        validate_graph(graph)


@pytest.mark.trio
async def test_graph_forward_iteration():
    graph = LocalGraph(SGNode(1))

    for key in (3, 5):
        await graph.insert(key)

    assert tuple([key async for key in graph.iter_keys()]) == (1, 3, 5)
    assert tuple([key async for key in graph.iter_keys(start=1)]) == (1, 3, 5)
    assert tuple([key async for key in graph.iter_keys(start=2)]) == (3, 5)
    assert tuple([key async for key in graph.iter_keys(start=3)]) == (3, 5)
    assert tuple([key async for key in graph.iter_keys(start=4)]) == (5,)
    assert tuple([key async for key in graph.iter_keys(start=5)]) == (5,)
    assert tuple([key async for key in graph.iter_keys(start=6)]) == ()

    assert tuple([key async for key in graph.iter_keys(end=10)]) == (1, 3, 5)
    assert tuple([key async for key in graph.iter_keys(start=1, end=10)]) == (1, 3, 5)
    assert tuple([key async for key in graph.iter_keys(start=1, end=5)]) == (1, 3)
    assert tuple([key async for key in graph.iter_keys(start=1, end=4)]) == (1, 3)
    assert tuple([key async for key in graph.iter_keys(start=1, end=3)]) == (1,)
    assert tuple([key async for key in graph.iter_keys(start=2, end=3)]) == ()


@pytest.mark.trio
async def test_graph_reverse_iteration():
    graph = LocalGraph(SGNode(1))

    for key in (3, 5):
        await graph.insert(key)

    assert tuple([key async for key in graph.iter_keys(start=10, end=5)]) == ()
    assert tuple([key async for key in graph.iter_keys(start=10, end=4)]) == (5,)
    assert tuple([key async for key in graph.iter_keys(start=10, end=3)]) == (5,)
    assert tuple([key async for key in graph.iter_keys(start=10, end=2)]) == (5, 3)
    assert tuple([key async for key in graph.iter_keys(start=5, end=3)]) == (5,)
    assert tuple([key async for key in graph.iter_keys(start=5, end=2)]) == (5, 3)
    assert tuple([key async for key in graph.iter_keys(start=5, end=1)]) == (5, 3)
    assert tuple([key async for key in graph.iter_keys(start=5, end=0)]) == (5, 3, 1)
    assert tuple([key async for key in graph.iter_keys(start=4, end=0)]) == (3, 1)
    assert tuple([key async for key in graph.iter_keys(start=3, end=0)]) == (3, 1)
    assert tuple([key async for key in graph.iter_keys(start=2, end=0)]) == (1,)
    assert tuple([key async for key in graph.iter_keys(start=1, end=0)]) == (1,)
