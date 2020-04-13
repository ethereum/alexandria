import pytest

from alexandria.skip_graph import SGNode, Graph, AlreadyPresent

from hypothesis import (
    strategies as st,
    given,
)


def _assert_graph_validity(graph):
    for node in graph.nodes.values():
        _assert_node_validity(graph, node)


def _assert_node_validity(graph, node):
    assert node.key in graph.nodes
    _assert_node_left_neighbor_validity(graph, node)
    _assert_node_right_neighbor_validity(graph, node)


def _assert_node_left_neighbor_validity(graph, node):
    for level, left_neighbor_key in node.iter_down_left_levels(node.max_level):
        # sanity
        assert node.get_left_neighbor(level) == left_neighbor_key

        if level > 0 and level == node.max_level:
            # At the max level, both neighbors must be empty
            assert node.get_left_neighbor(level) is None
            assert node.get_right_neighbor(level) is None
        else:
            # At least one neighbor must be non-empty
            assert (
                node.get_left_neighbor(level) is not None or  # noqa: W504
                node.get_right_neighbor(level) is not None
            )

        if left_neighbor_key is None:
            continue

        # Make sure the left neighbor correctly thinks this node is its right neighbor
        assert left_neighbor_key < node.key
        left_neighbor = graph.nodes[left_neighbor_key]
        assert left_neighbor.get_right_neighbor(level) == node.key

        if level > 0:
            node_membership_vector = node.get_membership_at_level(level)
            left_neighbor_membership_vector = left_neighbor.get_membership_at_level(level)

            assert node_membership_vector == left_neighbor_membership_vector


def _assert_node_right_neighbor_validity(graph, node):
    for level, right_neighbor_key in node.iter_down_right_levels(node.max_level):
        # sanity
        assert node.get_right_neighbor(level) == right_neighbor_key

        if level > 0 and level == node.max_level:
            # At the max level, both neighbors must be empty
            assert node.get_right_neighbor(level) is None
            assert node.get_left_neighbor(level) is None
        else:
            # At least one neighbor must be non-empty
            assert (
                node.get_right_neighbor(level) is not None or  # noqa: W504
                node.get_left_neighbor(level) is not None
            )

        if right_neighbor_key is None:
            continue

        # Make sure the right neighbor correctly thinks this node is its left neighbor
        assert right_neighbor_key > node.key
        right_neighbor = graph.nodes[right_neighbor_key]
        assert right_neighbor.get_left_neighbor(level) == node.key

        if level > 0:
            node_membership_vector = node.get_membership_at_level(level)
            right_neighbor_membership_vector = right_neighbor.get_membership_at_level(level)

            assert node_membership_vector == right_neighbor_membership_vector


def test_search():
    anchor = SGNode(1234)
    graph = Graph(anchor)

    result = graph.search(anchor.key, anchor)
    assert result.key == anchor.key

    _assert_graph_validity(graph)


def test_insert_far_right():
    anchor = SGNode(0)
    graph = Graph(anchor)

    node = graph.insert(1, anchor)
    assert node.key == 1
    assert node.get_left_neighbor(0) == anchor.key
    assert node.get_right_neighbor(0) is None

    assert node.get_left_neighbor(node.max_level) is None
    assert node.get_right_neighbor(node.max_level) is None


def test_insert_far_left():
    anchor = SGNode(1)
    graph = Graph(anchor)

    node = graph.insert(0, anchor)
    assert node.key == 0
    assert node.get_right_neighbor(0) == anchor.key
    assert node.get_left_neighbor(0) is None

    _assert_graph_validity(graph)


@given(
    anchor_key=st.integers(min_value=0, max_value=2**32),
    keys_to_insert=st.lists(
        st.integers(min_value=0, max_value=2**32),
        min_size=1,
        max_size=1000,
    ),
)
def test_skip_graph_insert_fuzz(anchor_key, keys_to_insert):
    anchor = SGNode(anchor_key)
    graph = Graph(anchor)

    inserted = {anchor_key}

    for key in keys_to_insert:
        if key in inserted:
            with pytest.raises(AlreadyPresent):
                graph.insert(key, anchor)
        else:
            node = graph.insert(key, anchor)
            assert node.key == key
            inserted.add(key)
        _assert_graph_validity(graph)
