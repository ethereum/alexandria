from abc import abstractmethod
import logging
from typing import Dict, Iterator, KeysView, Optional, Sequence, Tuple

from eth_utils import int_to_big_endian, ValidationError

from alexandria._utils import content_key_to_node_id
from alexandria.abc import SGNodeAPI, GraphAPI, FindResult, NetworkAPI
from alexandria.typing import Key


class NotFound(Exception):
    pass


class AlreadyPresent(Exception):
    pass


class Missing(Exception):
    pass


class SGNode(SGNodeAPI):
    def __init__(self,
                 key: Key,
                 neighbors_left: Sequence[Key] = None,
                 neighbors_right: Sequence[Key] = None,
                 ) -> None:
        self.key = key
        self.membership_vector = content_key_to_node_id(int_to_big_endian(key))

        if neighbors_left is None:
            self.neighbors_left = []
        else:
            self.neighbors_left = list(neighbors_left)

        if neighbors_right is None:
            self.neighbors_right = []
        else:
            self.neighbors_right = list(neighbors_right)

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
        elif key >= self.key:
            raise ValidationError(f"Invalid left neighbor: {hex(key)} >= {hex(self.key)}")
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
        elif key <= self.key:
            raise ValidationError(f"Invalid left neighbor: {hex(key)} >= {hex(self.key)}")
        elif at_level == len(self.neighbors_right):
            self.neighbors_right.append(key)
        elif at_level < len(self.neighbors_right):
            self.neighbors_right[at_level] = key
        else:
            raise ValueError(f"Cannot set right neighbor at level #{at_level}: {key}")

    def iter_down_left_levels(self, from_level: int) -> Iterator[Tuple[int, Optional[Key]]]:
        for level in range(from_level, -1, -1):
            yield level, self.get_left_neighbor(level)

    def iter_down_right_levels(self, from_level: int) -> Iterator[Tuple[int, Optional[Key]]]:
        for level in range(from_level, -1, -1):
            yield level, self.get_right_neighbor(level)


class GraphDB:
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


class BaseGraph(GraphAPI):
    logger = logging.getLogger('alexandria.skip_graph.Graph')
    db: GraphDB

    @abstractmethod
    async def get_node(self, key: Key) -> SGNodeAPI:
        ...

    @abstractmethod
    async def link_nodes(self,
                         left: Optional[SGNodeAPI],
                         right: Optional[SGNodeAPI],
                         level: int) -> None:
        ...

    async def insert(self, key: Key, cursor: Optional[SGNodeAPI]) -> SGNodeAPI:
        if cursor is None:
            cursor = self.cursor
        self.logger.debug("Inserting: %d", key)
        left_neighbor, found, right_neighbor = await self._search(key, cursor, cursor.max_level)
        if found is not None:
            raise AlreadyPresent(f"Key already present: {found}")
        self.logger.debug("Insertion point found: %s < %d < %s", left_neighbor, key, right_neighbor)
        node = SGNode(key=key)
        return await self._insert_at_level(node, left_neighbor, right_neighbor, 0)

    async def _get_left_neighbor(self, node: SGNodeAPI, level: int) -> Optional[SGNodeAPI]:
        neighbor_key = node.get_left_neighbor(level)
        if neighbor_key is None:
            return None
        else:
            return await self.get_node(neighbor_key)

    async def _get_right_neighbor(self, node: SGNodeAPI, level: int) -> Optional[SGNodeAPI]:
        neighbor_key = node.get_right_neighbor(level)
        if neighbor_key is None:
            return None
        else:
            return await self.get_node(neighbor_key)

    async def _insert_at_level(self,
                               node: SGNodeAPI,
                               left: Optional[SGNodeAPI],
                               right: Optional[SGNodeAPI],
                               level: int) -> SGNodeAPI:
        # Link the nodes on the cursor level
        await self._link_nodes(left, node, level)
        await self._link_nodes(node, right, level)

        self.logger.debug("Level-%d: (%s < %s < %s)", level, left, node, right)

        # Break if both neighbors are now null
        if left is None and right is None:
            self.db.set(node.key, node)
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
            left = await self._get_left_neighbor(left, level)

        if left is not None:
            right = await self._get_right_neighbor(left, level + 1)
        else:
            while right is not None and right.get_membership_at_level(level + 1) != node_membership:
                right = await self._get_right_neighbor(right, level)

        return await self._insert_at_level(node, left, right, level + 1)

    async def _link_nodes(self,
                          left: Optional[SGNodeAPI],
                          right: Optional[SGNodeAPI],
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

    async def delete(self, key: Key, cursor: Optional[SGNodeAPI]) -> None:
        if cursor is None:
            cursor = self.cursor
        self.logger.debug('Deleting: %d', key)
        node = await self.search(key, cursor)

        if key == self.cursor.key:
            if node.get_right_neighbor(0) is not None:
                self.cursor = await self._get_right_neighbor(node, 0)
            elif node.get_left_neighbor(0) is not None:
                self.cursor = await self._get_left_neighbor(node, 0)
            else:
                raise Exception("Attempt to delete last remaining tree node")

        left: Optional[SGNodeAPI]
        right: Optional[SGNodeAPI]

        for level in range(node.max_level, -1, -1):
            left = await self._get_left_neighbor(node, level)
            right = await self._get_right_neighbor(node, level)

            # Remove the node from the linked list at this level, directly linking it's neighbors
            if left is not None or right is not None:
                await self._link_nodes(left, right, level)

        self.db.delete(key)

    async def search(self, key: Key, cursor: Optional[SGNodeAPI]) -> SGNodeAPI:
        if cursor is None:
            cursor = self.cursor
        self.logger.debug("Searching: %d", key)
        left, node, right = await self._search(key, cursor, cursor.max_level)
        if node is None:
            raise NotFound(
                f"Node not found for key={hex(key)}.  Closest neighbors were: {left} / {right}"
            )
        return node

    async def find(self,
                   key: Key,
                   cursor: Optional[SGNodeAPI],
                   ) -> FindResult:
        if cursor is None:
            cursor = self.cursor
        return await self._search(key, cursor, cursor.max_level)

    async def _search(self,
                      key: Key,
                      cursor: SGNodeAPI,
                      level: int) -> FindResult:
        if key == cursor.key:
            return None, cursor, None
        elif key > cursor.key:
            for at_level, right_key in cursor.iter_down_right_levels(level):
                if right_key is None:
                    continue

                if key >= right_key:
                    right_neighbor = await self.get_node(right_key)
                    return await self._search(key, right_neighbor, at_level)
            else:
                right_neighbor_key = cursor.get_right_neighbor(0)
                if right_neighbor_key is None:
                    return cursor, None, None
                right_neighbor = await self.get_node(right_neighbor_key)
                return cursor, None, right_neighbor
        elif key < cursor.key:
            for at_level, left_key in cursor.iter_down_left_levels(level):
                if left_key is None:
                    continue

                if key <= left_key:
                    left_neighbor = await self.get_node(left_key)
                    return await self._search(key, left_neighbor, at_level)
            else:
                left_neighbor_key = cursor.get_left_neighbor(0)
                if left_neighbor_key is None:
                    return None, None, cursor
                left_neighbor = await self.get_node(left_neighbor_key)
                return left_neighbor, None, cursor
        else:
            raise Exception("Invariant")


def _link_local_nodes(left: Optional[SGNodeAPI],
                      right: Optional[SGNodeAPI],
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


class LocalGraph(BaseGraph):
    def __init__(self, cursor: SGNodeAPI) -> None:
        self.cursor = cursor
        self.db = GraphDB()
        self.db.set(cursor.key, cursor)

    async def get_node(self, key: Key) -> SGNodeAPI:
        try:
            return self.node_db.get(key)
        except KeyError as err:
            raise Missing from err

    async def link_nodes(self,
                         left: Optional[SGNodeAPI],
                         right: Optional[SGNodeAPI],
                         level: int) -> None:
        _link_local_nodes(left, right, level)


class NetworkGraph(BaseGraph):
    def __init__(self,
                 db: GraphDB,
                 cursor: SGNodeAPI,
                 network: NetworkAPI) -> None:
        self.db = db
        self.db.set(cursor.key, cursor)

        self.cursor = cursor
        self._network = network

    async def get_node(self, key: Key) -> SGNodeAPI:
        try:
            return self.db.get(key)
        except KeyError:
            try:
                return await self.network.get_node(key)
            except NotFound as err:
                raise Missing from err

    async def link_nodes(self,
                         left: Optional[SGNodeAPI],
                         right: Optional[SGNodeAPI],
                         level: int) -> None:
        _link_local_nodes(left, right, level)
        await self.network.link_nodes(left, right, level)
