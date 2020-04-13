import pytest

from alexandria.skip_graph import SGNode


def test_single_node():
    node = SGNode(0)

    assert node.max_level == 0

    for level in range(256):
        assert node.get_left_neighbor(level) is None
        assert node.get_right_neighbor(level) is None

    assert tuple(node.iter_down_left_levels(0)) == ((0, None),)
    assert tuple(node.iter_down_right_levels(0)) == ((0, None),)


def test_single_node_with_only_right_neighbor():
    node = SGNode(0, neighbors_right=[1])

    assert node.max_level == 1

    assert node.get_left_neighbor(0) is None
    assert node.get_right_neighbor(0) == 1

    for level in range(1, 256):
        assert node.get_left_neighbor(level) is None
        assert node.get_right_neighbor(level) is None

    assert tuple(node.iter_down_left_levels(0)) == ((0, None),)
    assert tuple(node.iter_down_right_levels(0)) == ((0, 1),)

    assert tuple(node.iter_down_left_levels(1)) == ((1, None), (0, None))
    assert tuple(node.iter_down_right_levels(1)) == ((1, None), (0, 1))


def test_single_node_with_only_left_neighbor():
    node = SGNode(1, neighbors_left=[0])

    assert node.max_level == 1

    assert node.get_left_neighbor(0) == 0
    assert node.get_right_neighbor(0) is None

    for level in range(1, 256):
        assert node.get_left_neighbor(level) is None
        assert node.get_right_neighbor(level) is None

    assert tuple(node.iter_down_left_levels(0)) == ((0, 0),)
    assert tuple(node.iter_down_right_levels(0)) == ((0, None),)

    assert tuple(node.iter_down_left_levels(1)) == ((1, None), (0, 0))
    assert tuple(node.iter_down_right_levels(1)) == ((1, None), (0, None))


def test_single_node_with_both_neighbors():
    node = SGNode(4, neighbors_left=[2, 0], neighbors_right=[8, 12, 28])

    assert node.max_level == 3

    assert node.get_left_neighbor(0) == 2
    assert node.get_right_neighbor(0) == 8

    assert node.get_left_neighbor(1) == 0
    assert node.get_right_neighbor(1) == 12

    assert node.get_left_neighbor(2) is None
    assert node.get_right_neighbor(2) == 28

    for level in range(3, 256):
        assert node.get_left_neighbor(level) is None
        assert node.get_right_neighbor(level) is None

    assert tuple(node.iter_down_left_levels(0)) == ((0, 2),)
    assert tuple(node.iter_down_right_levels(0)) == ((0, 8),)

    assert tuple(node.iter_down_left_levels(1)) == ((1, 0), (0, 2))
    assert tuple(node.iter_down_right_levels(1)) == ((1, 12), (0, 8))

    assert tuple(node.iter_down_left_levels(2)) == ((2, None), (1, 0), (0, 2))
    assert tuple(node.iter_down_right_levels(2)) == ((2, 28), (1, 12), (0, 8))

    assert tuple(node.iter_down_left_levels(3)) == ((3, None), (2, None), (1, 0), (0, 2))
    assert tuple(node.iter_down_right_levels(3)) == ((3, None), (2, 28), (1, 12), (0, 8))


def test_single_node_neighbor_setting():
    node = SGNode(4)

    assert node.max_level == 0
    assert node.get_left_neighbor(0) is None
    assert node.get_right_neighbor(0) is None

    node.set_left_neighbor(0, 2)

    assert node.max_level == 1
    assert node.get_left_neighbor(0) == 2
    assert node.get_right_neighbor(0) is None

    node.set_left_neighbor(1, 0)

    assert node.max_level == 2
    assert node.get_left_neighbor(0) == 2
    assert node.get_left_neighbor(1) == 0
    assert node.get_right_neighbor(0) is None

    # should be an error to set a neighbor above the topmost non-null neighbor.
    with pytest.raises(ValueError):
        node.set_right_neighbor(1, 12)
    with pytest.raises(ValueError):
        node.set_right_neighbor(2, 28)

    node.set_right_neighbor(0, 8)

    with pytest.raises(ValueError):
        node.set_right_neighbor(2, 28)

    assert node.max_level == 2
    assert node.get_right_neighbor(0) == 8
    assert node.get_right_neighbor(1) is None

    node.set_right_neighbor(1, 12)
    node.set_right_neighbor(2, 28)

    assert node.max_level == 3
    assert node.get_right_neighbor(0) == 8
    assert node.get_right_neighbor(1) == 12
    assert node.get_right_neighbor(2) == 28
    assert node.get_right_neighbor(3) is None
    assert node.get_right_neighbor(4) is None
    assert node.get_right_neighbor(5) is None
    assert node.get_right_neighbor(6) is None

    node.set_right_neighbor(3, None)
    node.set_right_neighbor(4, None)
    node.set_right_neighbor(5, None)
    node.set_right_neighbor(6, None)

    assert node.max_level == 3
    assert node.get_right_neighbor(0) == 8
    assert node.get_right_neighbor(1) == 12
    assert node.get_right_neighbor(2) == 28
    assert node.get_right_neighbor(3) is None
    assert node.get_right_neighbor(4) is None
    assert node.get_right_neighbor(5) is None
    assert node.get_right_neighbor(6) is None

    with pytest.raises(ValueError):
        node.set_right_neighbor(1, None)
    with pytest.raises(ValueError):
        node.set_right_neighbor(0, None)
    node.set_right_neighbor(2, None)

    assert node.max_level == 2
    assert node.get_right_neighbor(0) == 8
    assert node.get_right_neighbor(1) == 12
    assert node.get_right_neighbor(2) is None
    assert node.get_right_neighbor(3) is None
    assert node.get_right_neighbor(4) is None
    assert node.get_right_neighbor(5) is None
    assert node.get_right_neighbor(6) is None

    with pytest.raises(ValueError):
        node.set_right_neighbor(0, None)
    node.set_right_neighbor(1, None)

    assert node.max_level == 2
    assert node.get_right_neighbor(0) == 8
    assert node.get_right_neighbor(1) is None
    assert node.get_right_neighbor(2) is None
    assert node.get_right_neighbor(3) is None
    assert node.get_right_neighbor(4) is None
    assert node.get_right_neighbor(5) is None
    assert node.get_right_neighbor(6) is None

    node.set_right_neighbor(0, 28)

    assert node.max_level == 2
    assert node.get_right_neighbor(0) == 28
    assert node.get_right_neighbor(1) is None
    assert node.get_right_neighbor(2) is None
    assert node.get_right_neighbor(3) is None
    assert node.get_right_neighbor(4) is None
    assert node.get_right_neighbor(5) is None
    assert node.get_right_neighbor(6) is None

    node.set_right_neighbor(0, None)

    assert node.max_level == 2
    assert node.get_right_neighbor(0) is None
    assert node.get_right_neighbor(1) is None
    assert node.get_right_neighbor(2) is None
    assert node.get_right_neighbor(3) is None
    assert node.get_right_neighbor(4) is None
    assert node.get_right_neighbor(5) is None
    assert node.get_right_neighbor(6) is None


def test_node_get_membership_at_level():
    node = SGNode(1234)

    assert node.get_membership_at_level(0) == 0
    assert node.get_membership_at_level(1) == 1
    assert node.get_membership_at_level(2) == 3
    assert node.get_membership_at_level(3) == 3
    assert node.get_membership_at_level(4) == 11
