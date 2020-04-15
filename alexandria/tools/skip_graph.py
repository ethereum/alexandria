from alexandria.abc import GraphAPI, SGNodeAPI


def validate_graph(graph: GraphAPI) -> None:
    for node in graph.db._db.values():
        validate_node(graph, node)


def validate_node(graph: GraphAPI, node: SGNodeAPI) -> None:
    assert node.key in graph.db._db
    _assert_node_left_neighbor_validity(graph, node)
    _assert_node_right_neighbor_validity(graph, node)


def _assert_node_left_neighbor_validity(graph: GraphAPI, node: SGNodeAPI) -> None:
    for level, left_neighbor_key in node.iter_down_left_levels(node.max_level):
        # sanity
        assert node.get_left_neighbor(level) == left_neighbor_key

        if level == node.max_level:
            # At the max level, both neighbors must be empty
            assert node.get_left_neighbor(level) is None
            assert node.get_right_neighbor(level) is None
        elif level > 0:
            # At least one neighbor must be non-empty
            assert (
                node.get_left_neighbor(level) is not None or  # noqa: W504
                node.get_right_neighbor(level) is not None
            )

        if left_neighbor_key is None:
            continue

        # Make sure the left neighbor correctly thinks this node is its right neighbor
        assert left_neighbor_key < node.key
        left_neighbor = graph.db.get(left_neighbor_key)
        assert left_neighbor.get_right_neighbor(level) == node.key

        if level > 0:
            node_membership_vector = node.get_membership_at_level(level)
            left_neighbor_membership_vector = left_neighbor.get_membership_at_level(level)

            assert node_membership_vector == left_neighbor_membership_vector


def _assert_node_right_neighbor_validity(graph: GraphAPI, node: SGNodeAPI) -> None:
    for level, right_neighbor_key in node.iter_down_right_levels(node.max_level):
        # sanity
        assert node.get_right_neighbor(level) == right_neighbor_key

        if level == node.max_level:
            # At the max level, both neighbors must be empty
            assert node.get_right_neighbor(level) is None
            assert node.get_left_neighbor(level) is None
        elif level > 0:
            # At least one neighbor must be non-empty
            assert (
                node.get_right_neighbor(level) is not None or  # noqa: W504
                node.get_left_neighbor(level) is not None
            )

        if right_neighbor_key is None:
            continue

        # Make sure the right neighbor correctly thinks this node is its left neighbor
        assert right_neighbor_key > node.key
        right_neighbor = graph.db.get(right_neighbor_key)
        assert right_neighbor.get_left_neighbor(level) == node.key

        if level > 0:
            node_membership_vector = node.get_membership_at_level(level)
            right_neighbor_membership_vector = right_neighbor.get_membership_at_level(level)

            assert node_membership_vector == right_neighbor_membership_vector
