import bisect
import operator
import random

from eth_utils.toolz import accumulate

from alexandria._utils import content_key_to_node_id
from alexandria.content_manager import EphemeralDB
from alexandria.routing_table import compute_distance

from hypothesis import (
    strategies as st,
    given,
)


def test_ephemeral_db_basics():
    center_id = 0
    keys = tuple(
        b'key/%d' % idx
        for idx in range(5)
    )
    key_a, key_b, key_c, key_d, key_e = tuple(sorted(
        keys,
        key=lambda key: compute_distance(center_id, content_key_to_node_id(key)),
    ))

    db = EphemeralDB(
        capacity=20,
        distance_fn=lambda key: compute_distance(center_id, content_key_to_node_id(key)),
    )
    assert db.has_capacity
    # first key fills the whole database... and is immediately evicted.
    db.set(key_e, b'0' * 21)
    assert db.has_capacity
    assert not db.has(key_e)

    # now get the database at to capacity
    db.set(key_a, b'0' * 10)
    db.set(key_c, b'0' * 10)
    assert not db.has_capacity
    assert db.capacity == 0
    assert db.has(key_a)
    assert db.has(key_c)

    # reinsertion of either key should not change things
    db.set(key_a, b'0' * 10)
    db.set(key_c, b'0' * 10)
    assert not db.has_capacity
    assert db.capacity == 0
    assert db.has(key_a)
    assert db.has(key_c)

    # inserting a key that is further away will be immediately evicted (no change)
    db.set(key_d, b'0')
    assert not db.has(key_d)
    assert not db.has_capacity
    assert db.capacity == 0
    assert db.has(key_a)
    assert db.has(key_c)

    # inserting a closer key will evict the furthest key
    db.set(key_b, b'0')
    assert not db.has(key_c)  # should be evicted
    assert db.has(key_a)
    assert db.has(key_b)


@given(
    capacity=st.integers(min_value=100, max_value=350),
)
def test_ephemeral_db_fuzz(capacity):
    center_id = 0
    keys = tuple(
        b'key/%d' % idx
        for idx in range(50)
    )
    values = tuple(
        '0' * idx
        for idx in range(1, 51)
    )
    items = list(zip(keys, values))
    lookup = dict(items)
    sorted_items = list(sorted(
        items,
        key=lambda item: compute_distance(center_id, content_key_to_node_id(item[0])),
    ))
    cumulative_sizes = tuple(accumulate(operator.add, (len(item[1]) for item in sorted_items)))
    cutoff_index = bisect.bisect_left(cumulative_sizes, capacity)
    remaining_capacity = capacity - cumulative_sizes[cutoff_index - 1]

    sorted_keys = tuple(key for key, value in sorted_items)
    expected_keys = sorted_keys[:cutoff_index]
    expected_evicted_keys = sorted_keys[cutoff_index:]

    random.shuffle(items)
    db = EphemeralDB(
        capacity=capacity,
        distance_fn=lambda key: compute_distance(center_id, content_key_to_node_id(key)),
    )

    for key, value in items:
        db.set(key, value)

    for key in expected_keys:
        assert db.has(key)
    for key in expected_evicted_keys:
        # we can end up with extra keys if the database has extra capacity that
        # don't end up getting evicted.
        if len(lookup[key]) <= remaining_capacity:
            continue
        assert not db.has(key)
