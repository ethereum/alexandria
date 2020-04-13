import random

import pytest
import trio

from alexandria.tools.skip_graph import validate_graph
from alexandria.skip_graph import SGNode, Graph, AlreadyPresent, NotFound

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
    graph = Graph(anchor)

    inserted = {anchor_key}

    for key in keys_to_insert:
        anchor_from = graph.nodes[random.choice(tuple(inserted))]
        if key in inserted:
            with pytest.raises(AlreadyPresent):
                await graph.insert(key, anchor_from)
        else:
            node = await graph.insert(key, anchor_from)
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
    graph = Graph(anchor)

    inserted = {anchor_key}

    for key in keys_to_insert:
        anchor_from = graph.nodes[random.choice(tuple(inserted))]
        if key in inserted:
            with pytest.raises(AlreadyPresent):
                await graph.insert(key, anchor_from)
        else:
            node = await graph.insert(key, anchor_from)
            assert node.key == key
            inserted.add(key)

    validate_graph(graph)

    for key in keys_to_search:
        search_anchor = graph.nodes[random.choice(tuple(inserted))]

        if key == anchor_key or key in keys_to_insert:
            node = await graph.search(key, search_anchor)
            assert node.key == key
        else:
            with pytest.raises(NotFound):
                await graph.search(key, search_anchor)
