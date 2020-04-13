from typing import Dict, Iterable, List, NewType, Optional, Tuple

from eth_utils import to_hex, int_to_big_endian

from alexandria._utils import content_key_to_node_id
from alexandria.typing import NodeID


class NotFound(Exception):
    pass


class AlreadyPresent(Exception):
    pass


Key = NewType('Key', int)


class SGNode:
    key: Key
    neighbors_left: List[Optional[Key]]
    neighbors_right: List[Optional[Key]]
    membership_vector: NodeID
    max_level: int
    delete_flag: bool

    def __init__(self,
                 key: Key,
                 neighbors_left: List[Optional[Key]] = None,
                 neighbors_right: List[Optional[Key]] = None,
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
        return to_hex(self.key)

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
            max(len(self.neighbors_left), len(self.neighbors_right)) - 1,
        )

    def get_membership_at_level(self, at_level: int) -> int:
        assert at_level > 0
        return self.membership_vector & 2**(at_level - 1)

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
        if len(self.neighbors_left) > at_level:
            self.neighbors_left[at_level] = key
        elif len(self.neighbors_left) == at_level:
            self.neighbors_left.append(key)
        else:
            raise Exception(f"Level too high: has {len(self.neighbors_left)} levels < {at_level}")

    def set_right_neighbor(self, at_level: int, key: Optional[Key]) -> None:
        if len(self.neighbors_right) > at_level:
            self.neighbors_right[at_level] = key
        elif len(self.neighbors_right) == at_level:
            self.neighbors_right.append(key)
        else:
            raise Exception(f"Level too high: has {len(self.neighbors_right)} levels < {at_level}")

    def iter_down_left_levels(self, from_level: int) -> Iterable[Tuple[int, Optional[Key]]]:
        yield from zip(range(from_level, -1, -1), reversed(self.neighbors_left))

    def iter_down_right_levels(self, from_level: int) -> Iterable[Tuple[int, Optional[Key]]]:
        yield from zip(range(from_level, -1, -1), reversed(self.neighbors_right))


class Graph:
    nodes: Dict[Key, SGNode]

    def __init__(self, initial_node: SGNode) -> None:
        self.nodes = {initial_node.key: initial_node}

    def insert(self, key: Key, current: SGNode) -> SGNode:
        left_neighbor, right_neighbor = self._search_insert_point(key, current, current.max_level)
        node = SGNode(key=key)

        if left_neighbor is None:
            node.set_left_neighbor(0, None)
        else:
            self._insert_left(node, left_neighbor, 0)

        if right_neighbor is None:
            node.set_right_neighbor(0, None)
        else:
            self._insert_right(node, right_neighbor, 0)

        self.nodes[key] = node
        return node

    def _insert_left(self,
                     node: SGNode,
                     left_neighbor: SGNode,
                     level: int) -> None:
        node.set_left_neighbor(level, left_neighbor.key)
        left_neighbor.set_right_neighbor(level, node.key)

        membership_vector_at_next_level = node.get_membership_at_level(level + 1)
        while True:
            if left_neighbor.get_membership_at_level(level + 1) == membership_vector_at_next_level:
                self._insert_left(node, left_neighbor, level + 1)
                self._insert_right(left_neighbor, node, level + 1)
                break

            next_neighbor_key = left_neighbor.get_left_neighbor(level)
            if next_neighbor_key is None:
                node.set_left_neighbor(level + 1, None)
                break

            left_neighbor = self.nodes[next_neighbor_key]

    def _insert_right(self,
                      node: SGNode,
                      right_neighbor: Optional[SGNode],
                      level: int) -> None:
        node.set_right_neighbor(level, right_neighbor.key)
        right_neighbor.set_left_neighbor(level, node.key)

        membership_vector_at_next_level = node.get_membership_at_level(level + 1)
        while True:
            if right_neighbor.get_membership_at_level(level + 1) == membership_vector_at_next_level:
                self._insert_right(node, right_neighbor, level + 1)
                self._insert_left(right_neighbor, node, level + 1)
                break

            next_neighbor_key = right_neighbor.get_right_neighbor(level)
            if next_neighbor_key is None:
                node.set_right_neighbor(level + 1, None)
                break

            right_neighbor = self.nodes[next_neighbor_key]

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
                    right_neighbor = self.nodes[right_key]
                    return self._search_insert_point(key, right_neighbor, at_level)
            else:
                right_neighbor_key = current.get_right_neighbor(level)
                if right_neighbor_key is None:
                    return current, None
                right_neighbor = self.nodes[right_neighbor_key]
                return current, right_neighbor
        elif key < current.key:
            for at_level, left_key in current.iter_down_left_levels(level):
                if left_key is None:
                    continue

                if key <= left_key:
                    left_neighbor = self.nodes[left_key]
                    return self._search_insert_point(key, left_neighbor, at_level)
            else:
                left_neighbor_key = current.get_left_neighbor(level)
                if left_neighbor_key is None:
                    return None, current
                left_neighbor = self.nodes[left_neighbor_key]
                return left_neighbor, current
        else:
            raise Exception("Invariant")

    def delete(self, key: Key, current: SGNode) -> None:
        ...

    def search(self, key: Key, current: SGNode) -> None:
        return self._search(key, current, current.max_level)

    def _search(self, key: Key, current: SGNode, at_level: int) -> SGNode:
        if key == current.key:
            return current
        elif key > current.key:
            for level, right_key in current.iter_down_right_levels(at_level):
                if right_key is None:
                    continue

                if key <= right_key:
                    right_neighbor = self.nodes[right_key]
                    return self._search(key, right_neighbor, level)
            else:
                raise NotFound(f"Search terminated at node={current}  level={at_level}")
        elif key < current.key:
            for level, left_key in current.iter_down_left_levels(at_level):
                if left_key is None:
                    continue

                if key >= left_key:
                    left_neighbor = self.nodes[left_key]
                    return self._search(key, left_neighbor, level)
            else:
                raise NotFound(f"Search terminated at node={current}  level={at_level}")
        else:
            raise Exception("Invariant")
