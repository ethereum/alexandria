from abc import ABC, abstractmethod
import logging
from typing import Dict, Iterable, List, NewType, Optional, Tuple

from eth_utils import int_to_big_endian

from alexandria._utils import content_key_to_node_id
from alexandria.typing import NodeID


class NotFound(Exception):
    pass


class AlreadyPresent(Exception):
    pass


Key = NewType('Key', int)


class SGNode:
    key: Key
    neighbors_left: List[Key]
    neighbors_right: List[Key]
    membership_vector: NodeID
    max_level: int
    delete_flag: bool

    def __init__(self,
                 key: Key,
                 neighbors_left: List[Key] = None,
                 neighbors_right: List[Key] = None,
                 ) -> None:
        self.key = key
        self.membership_vector = content_key_to_node_id(int_to_big_endian(key))

        if neighbors_left is None:
            neighbors_left = []
        self.neighbors_left = neighbors_left

        if neighbors_right is None:
            neighbors_right = []
        self.neighbors_right = neighbors_right

    def __str__(self) -> str:
        return str(self.key)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"key={self.key!r}, "
            f"neighbors_left={self.neighbors_left!r}, "
            f"neighbors_right={self.neighbors_right!r}"
            f")"
        )

    @property
    def max_level(self) -> int:
        return max(
            0,
            max(len(self.neighbors_left), len(self.neighbors_right))
        )

    def get_membership_at_level(self, at_level: int) -> int:
        return self.membership_vector & (2**at_level - 1)

    def get_right_neighbor(self, at_level: int) -> Optional[Key]:
        try:
            return self.neighbors_right[at_level]
        except IndexError:
            return None

    def get_left_neighbor(self, at_level: int) -> Optional[Key]:
        try:
            return self.neighbors_left[at_level]
        except IndexError:
            return None

    def set_left_neighbor(self, at_level: int, key: Optional[Key]) -> None:
        if key is None:
            if at_level == len(self.neighbors_left) - 1:
                self.neighbors_left.pop()
            elif at_level < len(self.neighbors_left):
                raise ValueError(f"Cannot set null level ")
            else:
                pass
        elif at_level == len(self.neighbors_left):
            self.neighbors_left.append(key)
        elif at_level < len(self.neighbors_left):
            self.neighbors_left[at_level] = key
        else:
            raise ValueError(f"Cannot set left neighbor at level #{at_level}: {key}")

    def set_right_neighbor(self, at_level: int, key: Optional[Key]) -> None:
        if key is None:
            if at_level == len(self.neighbors_right) - 1:
                self.neighbors_right.pop()
            elif at_level < len(self.neighbors_right):
                raise ValueError(f"Cannot set null level ")
            else:
                pass
        elif at_level == len(self.neighbors_right):
            self.neighbors_right.append(key)
        elif at_level < len(self.neighbors_right):
            self.neighbors_right[at_level] = key
        else:
            raise ValueError(f"Cannot set right neighbor at level #{at_level}: {key}")

    def iter_down_left_levels(self, from_level: int) -> Iterable[Tuple[int, Optional[Key]]]:
        for level in range(from_level, -1, -1):
            yield level, self.get_left_neighbor(level)

    def iter_down_right_levels(self, from_level: int) -> Iterable[Tuple[int, Optional[Key]]]:
        for level in range(from_level, -1, -1):
            yield level, self.get_right_neighbor(level)


class GraphAPI(ABC):
    @abstractmethod
    async def insert(self, key: Key, anchor: SGNode) -> SGNode:
        ...

    @abstractmethod
    async def delete(self, key: Key, anchor: SGNode) -> SGNode:
        ...

    @abstractmethod
    async def search(self, key: Key, anchor: SGNode) -> SGNode:
        ...


class Graph(GraphAPI):
    logger = logging.getLogger('alexandria.skip_graph.Graph')

    nodes: Dict[Key, SGNode]

    def __init__(self, initial_node: SGNode) -> None:
        self.nodes = {initial_node.key: initial_node}

    async def insert(self, key: Key, current: SGNode) -> SGNode:
        self.logger.debug("Inserting: %d", key)
        left_neighbor, right_neighbor = self._search_insert_point(key, current, current.max_level)
        self.logger.debug("Insertion point found: %s < %d < %s", left_neighbor, key, right_neighbor)
        node = SGNode(key=key)
        return self._insert_at_level(node, left_neighbor, right_neighbor, 0)

    def get_node(self, key: Key) -> SGNode:
        return self.nodes[key]

    def _insert_at_level(self,
                         node: SGNode,
                         left: Optional[SGNode],
                         right: Optional[SGNode],
                         level: int) -> SGNode:
        # Link the nodes on the current level
        self._link_nodes(left, node, level)
        self._link_nodes(node, right, level)

        self.logger.debug("Level-%d: (%s < %s < %s)", level, left, node, right)

        # Break if both neighbors are now null
        if left is None and right is None:
            self.nodes[node.key] = node
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
            next_left_key = left.get_left_neighbor(level)
            if next_left_key is None:
                left = None
                break
            else:
                left = self.get_node(next_left_key)

        while right is not None and right.get_membership_at_level(level + 1) != node_membership:
            next_right_key = right.get_right_neighbor(level)
            if next_right_key is None:
                right = None
                break
            else:
                right = self.get_node(next_right_key)

        return self._insert_at_level(node, left, right, level + 1)

    def _link_nodes(self,
                    left: Optional[SGNode],
                    right: Optional[SGNode],
                    level: int,
                    ) -> None:
        if left is None and right is None:
            raise Exception("Invariant")
        elif left is None:
            right.set_left_neighbor(level, None)
        elif right is None:
            left.set_right_neighbor(level, None)
        else:
            left.set_right_neighbor(level, right.key)
            right.set_left_neighbor(level, left.key)

    def _search_insert_point(self,
                             key: Key,
                             current: SGNode,
                             level: int) -> Tuple[Optional[SGNode], Optional[SGNode]]:
        if key == current.key:
            raise AlreadyPresent
        elif key > current.key:
            for at_level, right_key in current.iter_down_right_levels(level):
                if right_key is None:
                    continue

                if key >= right_key:
                    right_neighbor = self.get_node(right_key)
                    return self._search_insert_point(key, right_neighbor, at_level)
            else:
                right_neighbor_key = current.get_right_neighbor(0)
                if right_neighbor_key is None:
                    return current, None
                right_neighbor = self.get_node(right_neighbor_key)
                return current, right_neighbor
        elif key < current.key:
            for at_level, left_key in current.iter_down_left_levels(level):
                if left_key is None:
                    continue

                if key <= left_key:
                    left_neighbor = self.get_node(left_key)
                    return self._search_insert_point(key, left_neighbor, at_level)
            else:
                left_neighbor_key = current.get_left_neighbor(0)
                if left_neighbor_key is None:
                    return None, current
                left_neighbor = self.get_node(left_neighbor_key)
                return left_neighbor, current
        else:
            raise Exception("Invariant")

    async def delete(self, key: Key, current: SGNode) -> None:
        self.logger.debug('Deleting: %d', key)
        node = await self.search(key, current)

        left: Optional[SGNode]
        right: Optional[SGNode]

        for level in range(node.max_level, -1, -1):
            left_neighbor_key = node.get_left_neighbor(level)
            if left_neighbor_key is None:
                left = None
            else:
                left = self.get_node(left_neighbor_key)

            right_neighbor_key = node.get_right_neighbor(level)
            if right_neighbor_key is None:
                right = None
            else:
                right = self.get_node(right_neighbor_key)

            if left is not None or right is not None:
                self._link_nodes(left, right, level)

        self.nodes.pop(key)

    async def search(self, key: Key, current: SGNode) -> None:
        self.logger.debug("Searching: %d", key)
        return self._search(key, current, current.max_level)

    def _search(self, key: Key, current: SGNode, level: int) -> SGNode:
        if key == current.key:
            return current
        elif key > current.key:
            for at_level, right_key in current.iter_down_right_levels(level):
                if right_key is None:
                    continue

                if key >= right_key:
                    right_neighbor = self.get_node(right_key)
                    return self._search(key, right_neighbor, at_level)
            else:
                raise NotFound
        elif key < current.key:
            for at_level, left_key in current.iter_down_left_levels(level):
                if left_key is None:
                    continue

                if key <= left_key:
                    left_neighbor = self.get_node(left_key)
                    return self._search(key, left_neighbor, at_level)
            else:
                raise NotFound
        else:
            raise Exception("Invariant")
