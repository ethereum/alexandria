from alexandria.abc import Direction, GraphAPI, SGNodeAPI


def validate_graph(graph: GraphAPI) -> None:
    for node in graph.db._db.values():
        validate_node(graph, node)


def validate_node(graph: GraphAPI, node: SGNodeAPI) -> None:
    assert node.key in graph.db._db

    # At the max level, both neighbors must be empty
    assert node.get_neighbor(node.max_level, Direction.left) is None
    assert node.get_neighbor(node.max_level, Direction.right) is None

    for level, direction, neighbor_key in node.iter_neighbors():
        assert neighbor_key is not None

        # sanity
        assert node.get_neighbor(level, direction) == neighbor_key

        assert direction.comparison_fn(neighbor_key, node.key)

        # Make sure the neighbor relationship is reflexive
        neighbor = graph.db.get(neighbor_key)
        assert neighbor.get_neighbor(level, direction.opposite) == node.key

        if level > 0:
            node_membership_vector = node.get_membership_at_level(level)
            neighbor_membership_vector = neighbor.get_membership_at_level(level)

            assert node_membership_vector == neighbor_membership_vector
