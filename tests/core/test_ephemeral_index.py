import pytest

from alexandria.abc import Location
from alexandria.content_manager import EphemeralIndex


def test_ephemeral_index():
    index = EphemeralIndex(0, capacity=6)

    # fill up the index
    index.add(Location(1, 1))
    index.add(Location(1, 5))
    index.add(Location(1, 10))

    index.add(Location(2, 1))

    index.add(Location(3, 1))
    index.add(Location(3, 2))

    assert index.capacity == 0
    assert set(index.get_index(1)) == {1, 5, 10}
    assert set(index.get_index(2)) == {1}
    assert set(index.get_index(3)) == {1, 2}

    # setting an existing value shouldn't trigger any evictions
    index.add(Location(3, 1))
    assert set(index.get_index(1)) == {1, 5, 10}
    assert set(index.get_index(2)) == {1}
    assert set(index.get_index(3)) == {1, 2}

    # now we set something that triggers eviction
    index.add(Location(2, 5))
    assert set(index.get_index(1)) == {1, 5, 10}
    assert set(index.get_index(2)) == {1, 5}
    assert set(index.get_index(3)) == {1}

    index.add(Location(2, 2))
    index.add(Location(2, 4))
    assert set(index.get_index(1)) == {1, 5, 10}
    assert set(index.get_index(2)) == {1, 2, 4}
    with pytest.raises(KeyError):
        index.get_index(3)

    index.add(Location(1, 6))
    index.add(Location(1, 5))
    index.add(Location(1, 4))
    index.add(Location(1, 3))
    index.add(Location(1, 2))
    assert set(index.get_index(1)) == {1, 2, 3, 4, 5, 6}
    with pytest.raises(KeyError):
        index.get_index(2)
    with pytest.raises(KeyError):
        index.get_index(3)
