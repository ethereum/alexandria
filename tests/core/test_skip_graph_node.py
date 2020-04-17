import pytest

from eth_utils import ValidationError

from alexandria.skip_graph import SGNode, LEFT, RIGHT


def test_single_node():
    node = SGNode(0)

    assert node.max_level == 0

    for level in range(256):
        assert node.get_neighbor(level, LEFT) is None
        assert node.get_neighbor(level, RIGHT) is None

    assert tuple(node.iter_down_levels(0, LEFT)) == ((0, None),)
    assert tuple(node.iter_down_levels(0, RIGHT)) == ((0, None),)


def test_single_node_with_only_right_neighbor():
    node = SGNode(0, neighbors_right=[1])

    assert node.max_level == 1

    assert node.get_neighbor(0, LEFT) is None
    assert node.get_neighbor(0, RIGHT) == 1

    for level in range(1, 256):
        assert node.get_neighbor(level, LEFT) is None
        assert node.get_neighbor(level, RIGHT) is None

    assert tuple(node.iter_down_levels(0, LEFT)) == ((0, None),)
    assert tuple(node.iter_down_levels(0, RIGHT)) == ((0, 1),)

    assert tuple(node.iter_down_levels(1, LEFT)) == ((1, None), (0, None))
    assert tuple(node.iter_down_levels(1, RIGHT)) == ((1, None), (0, 1))

    assert tuple(node.iter_neighbors()) == ((0, RIGHT, 1),)


def test_single_node_with_only_left_neighbor():
    node = SGNode(1, neighbors_left=[0])

    assert node.max_level == 1

    assert node.get_neighbor(0, LEFT) == 0
    assert node.get_neighbor(0, RIGHT) is None

    for level in range(1, 256):
        assert node.get_neighbor(level, LEFT) is None
        assert node.get_neighbor(level, RIGHT) is None

    assert tuple(node.iter_down_levels(0, LEFT)) == ((0, 0),)
    assert tuple(node.iter_down_levels(0, RIGHT)) == ((0, None),)

    assert tuple(node.iter_down_levels(1, LEFT)) == ((1, None), (0, 0))
    assert tuple(node.iter_down_levels(1, RIGHT)) == ((1, None), (0, None))

    assert tuple(node.iter_neighbors()) == ((0, LEFT, 0),)


def test_single_node_with_both_neighbors():
    node = SGNode(4, neighbors_left=[2, 0], neighbors_right=[8, 12, 28])

    assert node.max_level == 3

    assert node.get_neighbor(0, LEFT) == 2
    assert node.get_neighbor(0, RIGHT) == 8

    assert node.get_neighbor(1, LEFT) == 0
    assert node.get_neighbor(1, RIGHT) == 12

    assert node.get_neighbor(2, LEFT) is None
    assert node.get_neighbor(2, RIGHT) == 28

    for level in range(3, 256):
        assert node.get_neighbor(level, LEFT) is None
        assert node.get_neighbor(level, RIGHT) is None

    assert tuple(node.iter_down_levels(0, LEFT)) == ((0, 2),)
    assert tuple(node.iter_down_levels(0, RIGHT)) == ((0, 8),)

    assert tuple(node.iter_down_levels(1, LEFT)) == ((1, 0), (0, 2))
    assert tuple(node.iter_down_levels(1, RIGHT)) == ((1, 12), (0, 8))

    assert tuple(node.iter_down_levels(2, LEFT)) == ((2, None), (1, 0), (0, 2))
    assert tuple(node.iter_down_levels(2, RIGHT)) == ((2, 28), (1, 12), (0, 8))

    assert tuple(node.iter_down_levels(3, LEFT)) == ((3, None), (2, None), (1, 0), (0, 2))
    assert tuple(node.iter_down_levels(3, RIGHT)) == ((3, None), (2, 28), (1, 12), (0, 8))

    assert tuple(node.iter_neighbors()) == (
        (0, LEFT, 2),
        (0, RIGHT, 8),
        (1, LEFT, 0),
        (1, RIGHT, 12),
        (2, RIGHT, 28),
    )


def test_single_node_neighbor_setting():
    node = SGNode(4)

    assert node.max_level == 0
    assert node.get_neighbor(0, LEFT) is None
    assert node.get_neighbor(0, RIGHT) is None

    node.set_neighbor(0, LEFT, 2)

    assert node.max_level == 1
    assert node.get_neighbor(0, LEFT) == 2
    assert node.get_neighbor(0, RIGHT) is None

    node.set_neighbor(1, LEFT, 0)

    assert node.max_level == 2
    assert node.get_neighbor(0, LEFT) == 2
    assert node.get_neighbor(1, LEFT) == 0
    assert node.get_neighbor(0, RIGHT) is None

    # should be an error to set a neighbor above the topmost non-null neighbor.
    with pytest.raises(ValidationError):
        node.set_neighbor(1, RIGHT, 12)
    with pytest.raises(ValidationError):
        node.set_neighbor(2, LEFT, 28)

    node.set_neighbor(0, RIGHT, 8)

    with pytest.raises(ValidationError):
        node.set_neighbor(2, RIGHT, 28)

    assert node.max_level == 2
    assert node.get_neighbor(0, RIGHT) == 8
    assert node.get_neighbor(1, RIGHT) is None

    node.set_neighbor(1, RIGHT, 12)
    node.set_neighbor(2, RIGHT, 28)

    assert node.max_level == 3
    assert node.get_neighbor(0, RIGHT) == 8
    assert node.get_neighbor(1, RIGHT) == 12
    assert node.get_neighbor(2, RIGHT) == 28
    assert node.get_neighbor(3, RIGHT) is None
    assert node.get_neighbor(4, RIGHT) is None
    assert node.get_neighbor(5, RIGHT) is None
    assert node.get_neighbor(6, RIGHT) is None

    node.set_neighbor(3, RIGHT, None)
    node.set_neighbor(4, RIGHT, None)
    node.set_neighbor(5, RIGHT, None)
    node.set_neighbor(6, RIGHT, None)

    assert node.max_level == 3
    assert node.get_neighbor(0, RIGHT) == 8
    assert node.get_neighbor(1, RIGHT) == 12
    assert node.get_neighbor(2, RIGHT) == 28
    assert node.get_neighbor(3, RIGHT) is None
    assert node.get_neighbor(4, RIGHT) is None
    assert node.get_neighbor(5, RIGHT) is None
    assert node.get_neighbor(6, RIGHT) is None

    with pytest.raises(ValidationError):
        node.set_neighbor(1, RIGHT, None)
    with pytest.raises(ValidationError):
        node.set_neighbor(0, RIGHT, None)
    node.set_neighbor(2, RIGHT, None)

    assert node.max_level == 2
    assert node.get_neighbor(0, RIGHT) == 8
    assert node.get_neighbor(1, RIGHT) == 12
    assert node.get_neighbor(2, RIGHT) is None
    assert node.get_neighbor(3, RIGHT) is None
    assert node.get_neighbor(4, RIGHT) is None
    assert node.get_neighbor(5, RIGHT) is None
    assert node.get_neighbor(6, RIGHT) is None

    with pytest.raises(ValidationError):
        node.set_neighbor(0, RIGHT, None)
    node.set_neighbor(1, RIGHT, None)

    assert node.max_level == 2
    assert node.get_neighbor(0, RIGHT) == 8
    assert node.get_neighbor(1, RIGHT) is None
    assert node.get_neighbor(2, RIGHT) is None
    assert node.get_neighbor(3, RIGHT) is None
    assert node.get_neighbor(4, RIGHT) is None
    assert node.get_neighbor(5, RIGHT) is None
    assert node.get_neighbor(6, RIGHT) is None

    node.set_neighbor(0, RIGHT, 28)

    assert node.max_level == 2
    assert node.get_neighbor(0, RIGHT) == 28
    assert node.get_neighbor(1, RIGHT) is None
    assert node.get_neighbor(2, RIGHT) is None
    assert node.get_neighbor(3, RIGHT) is None
    assert node.get_neighbor(4, RIGHT) is None
    assert node.get_neighbor(5, RIGHT) is None
    assert node.get_neighbor(6, RIGHT) is None

    node.set_neighbor(0, RIGHT, None)

    assert node.max_level == 2
    assert node.get_neighbor(0, RIGHT) is None
    assert node.get_neighbor(1, RIGHT) is None
    assert node.get_neighbor(2, RIGHT) is None
    assert node.get_neighbor(3, RIGHT) is None
    assert node.get_neighbor(4, RIGHT) is None
    assert node.get_neighbor(5, RIGHT) is None
    assert node.get_neighbor(6, RIGHT) is None


def test_node_get_membership_at_level():
    node = SGNode(1234)

    assert node.get_membership_at_level(0) == 0
    assert node.get_membership_at_level(1) == 1
    assert node.get_membership_at_level(2) == 3
    assert node.get_membership_at_level(3) == 3
    assert node.get_membership_at_level(4) == 11
