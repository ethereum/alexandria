from abc import abstractmethod
import itertools
import logging
from typing import (
    AsyncIterator,
    Dict,
    Iterator,
    KeysView,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from eth_utils import int_to_big_endian, ValidationError
import trio

from alexandria._utils import content_key_to_node_id, graph_key_to_content_key
from alexandria.abc import (
    Direction,
    SGNodeAPI,
    GraphAPI,
    GraphDatabaseAPI,
    FindResult,
    NetworkAPI,
    Node,
)
from alexandria.constants import GET_GRAPH_NODE_TIMEOUT
from alexandria.typing import Key, NodeID


class NotFound(Exception):
    pass


class AlreadyPresent(Exception):
    pass


class Missing(Exception):
    pass


LEFT = Direction.left
RIGHT = Direction.right


class SGNode(SGNodeAPI):
    def __init__(self,
                 key: Key,
                 neighbors_left: Sequence[Key] = None,
                 neighbors_right: Sequence[Key] = None,
                 ) -> None:
        self.key = key
        self.membership_vector = content_key_to_node_id(int_to_big_endian(key))

        if neighbors_left is None:
            neighbors_left = []
        else:
            neighbors_left = list(neighbors_left)

        if neighbors_right is None:
            neighbors_right = []
        else:
            neighbors_right = list(neighbors_right)

        if not all(neighbor_key < key for neighbor_key in neighbors_left):
            raise ValidationError("Invalid left neighbors for key={hex(key)}: {neighbors_left}")
        if not all(neighbor_key > key for neighbor_key in neighbors_right):
            raise ValidationError("Invalid right neighbors for key={hex(key)}: {neighbors_right}")

        self.neighbors = (neighbors_left, neighbors_right)

    def __str__(self) -> str:
        return str(self.key)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"key={self.key!r}, "
            f"neighbors_left={self.neighbors[LEFT]!r}, "
            f"neighbors_right={self.neighbors[RIGHT]!r}"
            f")"
        )

    @property
    def max_level(self) -> int:
        return max(
            0,
            max(len(self.neighbors[LEFT]), len(self.neighbors[RIGHT]))
        )

    def get_membership_at_level(self, at_level: int) -> int:
        return self.membership_vector & (2**at_level - 1)  # type: ignore

    def get_neighbor(self, at_level: int, direction: Direction) -> Optional[Key]:
        neighbors = self.neighbors[direction]
        try:
            return neighbors[at_level]
        except IndexError:
            return None

    def set_neighbor(self, at_level: int, direction: Direction, key: Optional[Key]) -> None:
        neighbors = self.neighbors[direction]

        if key is not None and not direction.comparison_fn(key, self.key):
            raise ValidationError(
                "Invalid {direction.name} neighbor: key={hex(self.key)}  neighbor={hex(key)}"
            )

        if key is None:
            if at_level == len(neighbors) - 1:
                neighbors.pop()
            elif at_level < len(neighbors):
                raise ValidationError(
                    f"Invalid level for null neighbor: level={at_level} < "
                    f"num_{direction.name}_neighbors={len(neighbors)}"
                )
            else:
                pass
        elif not direction.comparison_fn(key, self.key):
            raise ValidationError(
                f"Invalid {direction.name} neighbor: {hex(key)} >= {hex(self.key)}"
            )
        elif at_level == len(neighbors):
            neighbors.append(key)
        elif at_level < len(neighbors):
            neighbors[at_level] = key
        else:
            raise ValidationError(
                f"Cannot set {direction.name} neighbor at level #{at_level}: {hex(key)}"
            )

    def iter_neighbors(self) -> Iterator[Tuple[int, Direction, Key]]:
        for level, (left, right) in enumerate(self.iter_neighbor_pairs()):
            if left is not None:
                yield level, LEFT, left
            if right is not None:
                yield level, RIGHT, right

    def iter_neighbor_pairs(self) -> Iterator[Tuple[Optional[Key], Optional[Key]]]:
        yield from itertools.zip_longest(*self.neighbors, fillvalue=None)
        yield None, None

    def iter_down_levels(self,
                         from_level: int,
                         direction: Direction,
                         ) -> Iterator[Tuple[int, Optional[Key]]]:
        for level in range(from_level, -1, -1):
            yield level, self.get_neighbor(level, direction)


class GraphDB(GraphDatabaseAPI):
    _db: Dict[Key, SGNodeAPI]

    def __init__(self) -> None:
        self._db = {}

    def keys(self) -> KeysView[Key]:
        return self._db.keys()

    def has(self, key: Key) -> bool:
        return key in self._db

    def get(self, key: Key) -> SGNodeAPI:
        return self._db[key]

    def set(self, key: Key, node: SGNodeAPI) -> None:
        self._db[key] = node

    def delete(self, key: Key) -> None:
        del self._db[key]


class GetNodeContext:
    cursor: SGNodeAPI
    level: int
    direction: Direction

    def __init__(self,
                 cursor: SGNodeAPI,
                 level: int,
                 direction: Direction,
                 ) -> None:
        self.cursor = cursor
        self.level = level
        self.direction = direction

    def is_reflexive_match(self, other: SGNodeAPI) -> bool:
        return other.get_neighbor(self.level, self.direction.opposite) == self.cursor.key


class BaseGraph(GraphAPI):
    logger = logging.getLogger('alexandria.skip_graph.Graph')
    cursor: Optional[SGNodeAPI]

    _lock: trio.Lock

    @property
    def has_cursor(self) -> bool:
        return hasattr(self, 'cursor')

    @abstractmethod
    async def get_node(self, key: Key, context: GetNodeContext) -> SGNodeAPI:
        ...

    @abstractmethod
    async def link_nodes(self,
                         left: Optional[SGNodeAPI],
                         right: Optional[SGNodeAPI],
                         level: int) -> None:
        ...

    async def insert(self, key: Key) -> SGNodeAPI:
        if not self.has_cursor:
            raise Exception("No database cursor")

        async with self._lock:
            self.logger.debug("Inserting: %d", key)
            left_neighbor, found, right_neighbor = await self._search(
                key,
                self.cursor,
                self.cursor.max_level,
            )
            if found is not None:
                raise AlreadyPresent(f"Key already present: {found}")
            self.logger.debug(
                "Insertion point found: %s < %d < %s",
                left_neighbor,
                key, right_neighbor,
            )
            node = SGNode(key=key)
            return await self._insert_at_level(node, left_neighbor, right_neighbor, 0)

    async def search(self, key: Key) -> SGNodeAPI:
        if not self.has_cursor:
            raise Exception("No database cursor")

        self.logger.debug("Searching: %d", key)
        left, node, right = await self._search(key, self.cursor, self.cursor.max_level)
        if node is None:
            raise NotFound(
                f"Node not found for key={hex(key)}.  Closest neighbors were: {left} / {right}"
            )
        return node

    async def find(self, key: Key) -> FindResult:
        if not self.has_cursor:
            raise Exception("No database cursor")
        return await self._search(key, self.cursor, self.cursor.max_level)

    async def delete(self, key: Key) -> None:
        if not self.has_cursor:
            raise Exception("No database cursor")

        async with self.lock:
            self.logger.debug('Deleting: %d', key)
            node = await self.search(key)

            if key == self.cursor.key:
                if node.get_neighbor(0, RIGHT) is not None:
                    self.cursor = await self._get_neighbor(node, 0, RIGHT)  # type: ignore
                elif node.get_neighbor(0, LEFT) is not None:
                    self.cursor = await self._get_neighbor(node, 0, LEFT)  # type: ignore
                else:
                    del self.cursor

            left: Optional[SGNodeAPI]
            right: Optional[SGNodeAPI]

            for level in range(node.max_level, -1, -1):
                left = await self._get_neighbor(node, level, LEFT)
                right = await self._get_neighbor(node, level, RIGHT)

                # Remove the node from the linked list at this level, directly
                # linking it's neighbors
                if left is not None or right is not None:
                    await self.link_nodes(left, right, level)

    async def iter_keys(self,
                        start: Key = Key(0),
                        end: Optional[Key] = None,
                        ) -> AsyncIterator[Key]:
        async for node in self.iter_values(start, end):
            yield node.key

    async def iter_values(self,
                          start: Key = Key(0),
                          end: Optional[Key] = None,
                          ) -> AsyncIterator[SGNodeAPI]:
        if end is None:
            direction = RIGHT
        elif end < start:
            direction = LEFT
        elif end >= start:
            direction = RIGHT
        else:
            raise Exception("Invariant")

        left, node, right = await self.find(start)
        if node is not None:
            cursor = node
        elif direction is RIGHT:
            cursor = right
        elif direction is LEFT:
            cursor = left
        else:
            raise Exception("Invariant")

        while cursor is not None:
            if end is not None and not direction.comparison_fn(end, cursor.key):
                break
            await trio.hazmat.checkpoint()
            yield cursor

            cursor = await self._get_neighbor(cursor, 0, direction)

    async def iter_items(self,
                         start: Key = Key(0),
                         end: Optional[Key] = None,
                         ) -> AsyncIterator[Tuple[Key, SGNodeAPI]]:
        async for node in self.iter_values(start, end):
            yield node.key, node

    #
    # Utility
    #
    async def _insert_at_level(self,
                               node: SGNodeAPI,
                               left: Optional[SGNodeAPI],
                               right: Optional[SGNodeAPI],
                               level: int) -> SGNodeAPI:
        # Link the nodes on the given level
        await self.link_nodes(left, node, level)
        await self.link_nodes(node, right, level)

        if left is not None:
            assert left.get_neighbor(level, RIGHT) == node.key
        if right is not None:
            assert right.get_neighbor(level, LEFT) == node.key

        self.logger.debug("Linked: level=%d (%s < %s < %s)", level, left, node, right)

        # Break if both neighbors are now null
        if left is None and right is None:
            return node

        # Now we iterate up the levels.  The `left` and
        # `right` values are neighbors on the current level.  We check
        # their membership vectors against the node's membership vector on the
        # next level.  If they match, we link them to each other on the next
        # level.  If not we scan through the neighbors on the current level
        # until we either find one that matches on the next level **or** we run
        # out of neighbors.
        node_membership = node.get_membership_at_level(level + 1)

        # Scan left until we find the first item in the list with a common membership vector
        while left is not None and left.get_membership_at_level(level + 1) != node_membership:
            # TODO: left neighbor needs to *maybe* have it's link on
            # the next level set, but this requires scanning back in
            # the other direction since it *might* link to something
            # else on the next level.  This suggests we need to avoid
            # populating the `null` levels and only have firm links.
            left = await self._get_neighbor(left, level, LEFT)

        if left is not None:
            right = await self._get_neighbor(left, level + 1, RIGHT)
        else:
            while right is not None and right.get_membership_at_level(level + 1) != node_membership:
                right = await self._get_neighbor(right, level, RIGHT)

        return await self._insert_at_level(node, left, right, level + 1)

    async def _search(self,
                      key: Key,
                      cursor: SGNodeAPI,
                      level: int) -> FindResult:
        if key == cursor.key:
            return None, cursor, None
        elif key > cursor.key:
            for at_level, right_key in cursor.iter_down_levels(level, RIGHT):
                if right_key is None:
                    continue

                if key >= right_key:
                    right_neighbor = await self.get_node(
                        right_key,
                        GetNodeContext(cursor, at_level, RIGHT),
                    )
                    return await self._search(key, right_neighbor, at_level)
            else:
                right_neighbor = await self._get_neighbor(cursor, 0, RIGHT)
                return cursor, None, right_neighbor
        elif key < cursor.key:
            for at_level, left_key in cursor.iter_down_levels(level, LEFT):
                if left_key is None:
                    continue

                if key <= left_key:
                    left_neighbor = await self.get_node(
                        left_key,
                        GetNodeContext(cursor, at_level, LEFT),
                    )
                    return await self._search(key, left_neighbor, at_level)
            else:
                left_neighbor = await self._get_neighbor(cursor, 0, LEFT)
                return left_neighbor, None, cursor
        else:
            raise Exception("Invariant")

    async def _get_neighbor(self,
                            node: SGNodeAPI,
                            level: int,
                            direction: Direction,
                            ) -> Optional[SGNodeAPI]:
        neighbor_key = node.get_neighbor(level, direction)
        if neighbor_key is None:
            return None
        else:
            return await self.get_node(neighbor_key, GetNodeContext(node, level, direction))


def _link_local_nodes(left: Optional[SGNodeAPI],
                      right: Optional[SGNodeAPI],
                      level: int,
                      ) -> None:
    if left is None and right is None:
        raise Exception("Invariant")
    elif left is None and right is not None:
        right.set_neighbor(level, LEFT, None)
    elif right is None and left is not None:
        left.set_neighbor(level, RIGHT, None)
    elif left is not None and right is not None:
        left.set_neighbor(level, RIGHT, right.key)
        right.set_neighbor(level, LEFT, left.key)
    else:
        raise Exception("Invariant")


class LocalGraph(BaseGraph):
    def __init__(self, cursor: Optional[SGNodeAPI] = None) -> None:
        self._lock = trio.Lock()
        self.db = GraphDB()
        if cursor is not None:
            self.cursor = cursor
            self.db.set(cursor.key, cursor)

    async def get_node(self, key: Key, context: GetNodeContext) -> SGNodeAPI:
        try:
            return self.db.get(key)
        except KeyError as err:
            raise Missing from err

    async def link_nodes(self,
                         left: Optional[SGNodeAPI],
                         right: Optional[SGNodeAPI],
                         level: int) -> None:
        _link_local_nodes(left, right, level)

    async def insert(self, key: Key) -> SGNodeAPI:
        if not self.has_cursor:
            self.cursor = SGNode(key)
            self.db.set(key, self.cursor)
            return self.cursor
        node = await super().insert(key)
        self.db.set(node.key, node)
        return node

    async def delete(self, key: Key) -> None:
        await super().delete(key)
        self.db.delete(key)


class NetworkGraph(BaseGraph):
    def __init__(self,
                 local_db: GraphDatabaseAPI,
                 cursor: Optional[SGNodeAPI],
                 network: NetworkAPI) -> None:
        self._lock = trio.Lock()
        self.db = local_db

        if cursor is not None:
            self.cursor = cursor

        self._network = network

    async def get_node(self, key: Key, context: GetNodeContext) -> SGNodeAPI:
        try:
            return self.db.get(key)
        except KeyError:
            try:
                return await self._network.get_node(key, context.is_reflexive_match)
            except NotFound as err:
                raise Missing from err

    async def link_nodes(self,
                         left: Optional[SGNodeAPI],
                         right: Optional[SGNodeAPI],
                         level: int) -> None:
        _link_local_nodes(left, right, level)


#
# Graph Fault Utilities
#
RawTraversalResult = Tuple[
    Tuple[Optional[Key], ...],
    Tuple[Optional[Key], ...],
]


NON_FAULTY_TERMINALS = {None, ...}


class TraversalResult:
    def __init__(self,
                 node: SGNodeAPI,
                 raw_traversal_result: RawTraversalResult,
                 ) -> None:
        self.node = node
        self._traversal_result = raw_traversal_result

    def __str__(self) -> str:
        if self.is_faulty:
            return f"TraversalResult[node={self.node} fault={self.fault_ratio} results={self._traversal_result}]"  # noqa: E501
        else:
            return f"TraversalResult[node={self.node} score={self.score} results={self._traversal_result}]"  # noqa: E501

    @property
    def is_faulty(self) -> bool:
        return any(_ends_with_fault(result) for result in itertools.chain(*self._traversal_result))

    @property
    def score(self) -> float:
        if self.is_faulty:
            return 0.0
        return sum(
            (len(left_result) + len(right_result)) / (level + 1)
            for level, (left_result, right_result)
            in enumerate(self._traversal_result)
        )

    @property
    def fault_ratio(self) -> float:
        if not self._traversal_result:
            return 1.0
        num_faults = len(tuple(
            traversal_result for traversal_result in itertools.chain(*self._traversal_result)
            if _ends_with_fault(traversal_result)
        ))
        num_results = len(self._traversal_result) * 2
        return num_faults / num_results


TraversalResults = Tuple[TraversalResult, ...]


def _ends_with_fault(traversal_result: TraversalResult) -> bool:
    if not traversal_result:
        return True
    return traversal_result[-1] not in NON_FAULTY_TERMINALS


async def get_traversal_result(network,
                               cursor: SGNodeAPI,
                               max_traversal_distance: int,
                               ) -> TraversalResult:
    raw_results = tuple([
        result async for result in traverse_neighbor_links(network, cursor, max_traversal_distance)
    ])
    return TraversalResult(cursor, raw_results)


async def traverse_neighbor_links(network,
                                  cursor: SGNodeAPI,
                                  max_traversal_distance: int,
                                  ) -> AsyncIterator[TraversalResult]:
    """
    Perform a breadth first walk through the graph until the first fault
    """
    for level in range(cursor.max_level):
        left_neighbor_keys = await scan_level(network, cursor, level, LEFT, max_traversal_distance)
        right_neighbor_keys = await scan_level(network, cursor, level, RIGHT, max_traversal_distance)  # noqa: E501
        yield left_neighbor_keys, right_neighbor_keys


async def scan_level(network: NetworkAPI,
                     cursor: SGNodeAPI,
                     level: int,
                     direction: Direction,
                     max_traversal_distance: int) -> Optional[int]:
    neighbor_key = cursor.get_neighbor(level, direction)
    if neighbor_key is None:
        # Terminal: End of list at this level
        return (None,)

    # All versions of the node for the given neighbor_key that we can find
    # on the network.
    neighbor_candidates = await get_node_versions(network, neighbor_key)
    if not neighbor_candidates:
        # Terminal: Neighbor key unretrievable from network
        return (neighbor_key,)

    # Filter those down to only the ones that share the reflexive
    # relationship.
    neighbors = tuple(
        neighbor for neighbor in neighbor_candidates
        if neighbor.get_neighbor(level, direction.opposite) == cursor.key
    )

    if not neighbors:
        # Terminal: None of the neighbor candidates at this level are
        # reflexively linked.
        return (neighbor_key,)

    if max_traversal_distance == 1:
        # Terminal: There is a valid neighbor but we are out of traversal distance
        return (neighbor_key, ...)
    elif max_traversal_distance <= 0:
        raise Exception("Invariant")

    neighbor_scan_results = []
    for neighbor in neighbors:
        # TODO: concurrency
        scan_result = await scan_level(network, neighbor, level, direction, max_traversal_distance - 1)  # noqa: E501
        neighbor_scan_results.append(scan_result)

    neighbor_scan_results_by_length = sorted(
        neighbor_scan_results,
        key=lambda result: len(result),
        reverse=True,
    )
    most_distant_scan = neighbor_scan_results_by_length[0]
    return (neighbor_key,) + most_distant_scan


async def get_node_versions(network: NetworkAPI,
                            key: Key) -> Tuple[SGNodeAPI, ...]:
    content_key = graph_key_to_content_key(key)
    content_id = content_key_to_node_id(content_key)
    nodes_near_content = await network.iterative_lookup(content_id)
    queried: Set[NodeID] = set()

    send_channel, receive_channel = trio.open_memory_channel[SGNodeAPI](2048)  # too big...

    async def do_get_graph_node(location: Node):
        if location.node_id == network.client.local_node_id:
            return
        with trio.move_on_after(GET_GRAPH_NODE_TIMEOUT):
            try:
                node = await network.get_graph_node(location, key=key)
            except NotFound:
                pass
            else:
                await send_channel.send(node)

    async with send_channel:
        async with trio.open_nursery() as nursery:
            for node in nodes_near_content:
                location_candidates = await network.locate(node, key=content_key)
                for location in location_candidates:
                    if location.node_id in queried:
                        continue
                    queried.add(location.node_id)
                    nursery.start_soon(do_get_graph_node, location)

    async with receive_channel:
        nodes = tuple([node async for node in receive_channel])

    return nodes
