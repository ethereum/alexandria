import bisect
import random

import pytest
import trio

from alexandria.tools.skip_graph import validate_graph
from alexandria.skip_graph import SGNode, LocalGraph, AlreadyPresent, NotFound

from hypothesis import (
    strategies as st,
    given,
)


@given(
    key=st.integers(min_value=0, max_value=2**32),
    level=st.integers(min_value=0, max_value=255),
)
def test_node_membership_vector(key, level):
    node = SGNode(key)
    membership_vector = node.get_membership_at_level(level)
    assert membership_vector.bit_length() <= level


@given(
    anchor_key=st.integers(min_value=0, max_value=2**32),
    keys_to_insert=st.lists(
        st.integers(min_value=0, max_value=2**32),
        min_size=1,
        max_size=1000,
    ),
    random_module=st.random_module(),
)
def test_skip_graph_insert_fuzz(anchor_key, keys_to_insert, random_module):
    trio.run(
        do_test_skip_graph_insert_fuzz,
        anchor_key,
        keys_to_insert,
        random_module,
    )


async def do_test_skip_graph_insert_fuzz(anchor_key, keys_to_insert, random_module):
    anchor = SGNode(anchor_key)
    graph = LocalGraph(anchor)

    inserted = {anchor_key}

    for key in keys_to_insert:
        graph.cursor = graph.db.get(random.choice(tuple(inserted)))
        if key in inserted:
            with pytest.raises(AlreadyPresent):
                await graph.insert(key)
        else:
            node = await graph.insert(key)
            assert node.key == key
            inserted.add(key)
        validate_graph(graph)


@given(
    anchor_key=st.integers(min_value=0, max_value=2**32),
    keys_to_insert=st.lists(
        st.integers(min_value=0, max_value=2**32),
        min_size=1,
        max_size=1000,
    ),
    keys_to_search=st.lists(
        st.integers(min_value=0, max_value=2**32),
        min_size=1,
        max_size=1000,
    ),
    random_module=st.random_module(),
)
def test_skip_graph_search_fuzz(anchor_key, keys_to_insert, keys_to_search, random_module):
    trio.run(
        do_test_skip_graph_search_fuzz,
        anchor_key,
        keys_to_insert,
        keys_to_search,
        random_module,
    )


async def do_test_skip_graph_search_fuzz(anchor_key, keys_to_insert, keys_to_search, random_module):
    anchor = SGNode(anchor_key)
    graph = LocalGraph(anchor)

    inserted = {anchor_key}

    for key in keys_to_insert:
        graph.cursor = graph.db.get(random.choice(tuple(inserted)))
        if key in inserted:
            with pytest.raises(AlreadyPresent):
                await graph.insert(key)
        else:
            node = await graph.insert(key)
            assert node.key == key
            inserted.add(key)

    validate_graph(graph)

    for key in keys_to_search:
        graph.cursor = graph.db._db[random.choice(tuple(inserted))]

        if key == anchor_key or key in keys_to_insert:
            node = await graph.search(key)
            assert node.key == key
        else:
            with pytest.raises(NotFound):
                await graph.search(key)


@given(
    raw_keys=st.lists(
        st.integers(min_value=0, max_value=2**32),
        min_size=1,
        max_size=1000,
    ),
    start=st.integers(min_value=0, max_value=2**32),
    end=st.one_of(st.none(), st.integers(min_value=0, max_value=2**32)),
)
def test_skip_graph_iteration(raw_keys, start, end):
    trio.run(
        do_test_skip_graph_iteration,
        raw_keys,
        start,
        end,
    )


async def do_test_skip_graph_iteration(raw_keys, start, end):
    ordered_keys = tuple(sorted(set(raw_keys)))
    if end is None:
        left_index = bisect.bisect_left(ordered_keys, start)
        right_index = None
    elif end >= start:
        left_index = bisect.bisect_left(ordered_keys, start)
        right_index = bisect.bisect_left(ordered_keys, end)
    elif end < start:
        left_index = bisect.bisect_right(ordered_keys, end)
        right_index = bisect.bisect_right(ordered_keys, start)
    else:
        raise Exception("Invariant")

    graph = LocalGraph(SGNode(ordered_keys[0]))

    for key in set(raw_keys).difference({ordered_keys[0]}):
        await graph.insert(key)

    if end is None or end >= start:
        expected_keys = ordered_keys[left_index:right_index]
    else:
        expected_keys = tuple(reversed(ordered_keys[left_index:right_index]))

    actual_items = tuple([(key, node) async for key, node in graph.iter_items(start, end)])
    actual_keys = tuple([key async for key in graph.iter_keys(start, end)])
    actual_values = tuple([node async for node in graph.iter_values(start, end)])

    assert len(actual_items) == len(expected_keys)
    assert len(actual_keys) == len(expected_keys)
    assert len(actual_values) == len(expected_keys)

    for key, (actual_key, node) in zip(expected_keys, actual_items):
        assert actual_key == key
        assert node.key == key

    for key, actual_key in zip(expected_keys, actual_keys):
        assert actual_key == key

    for key, node in zip(expected_keys, actual_values):
        assert node.key == key
