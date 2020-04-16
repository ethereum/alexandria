from typing import Collection, Iterator, Set, Tuple

from async_service import Service
from eth_utils import to_tuple
import trio

from alexandria._utils import graph_key_to_content_key, content_key_to_node_id
from alexandria.abc import Direction, GraphAPI, NetworkAPI, SGNodeAPI, Node
from alexandria.constants import GET_GRAPH_NODE_TIMEOUT
from alexandria.skip_graph import NotFound
from alexandria.typing import Key, NodeID


NeighborData = Tuple[int, Direction, Key, Tuple[SGNodeAPI, ...]]


class NodeAudit:
    def __init__(self,
                 node: SGNodeAPI,
                 neighbor_data: Collection[NeighborData],
                 ) -> None:
        self.node = node
        self.neighbor_data = neighbor_data

    @property
    @to_tuple
    def broken_neighbor_links(self) -> Iterator[Tuple[int, Direction]]:
        for level, direction, key, found_nodes in self.neighbor_data:
            if not found_nodes:
                continue

            for node in found_nodes:
                if node.get_neighbor(level, direction) == self.node.key:
                    break
            else:
                yield level, direction

    @property
    @to_tuple
    def missing_neighbors(self) -> Iterator[Tuple[int, Direction]]:
        for level, direction, key, found_nodes in self.neighbor_data:
            if not found_nodes:
                yield level, direction, key

    @property
    def is_valid(self) -> bool:
        return len(self.broken_neighbor_links) == 0 and len(self.missing_neighbors) == 0


class Mechanic(Service):
    def __init__(self, network: NetworkAPI, graph: GraphAPI) -> None:
        self.network = network
        self.graph = graph

    async def get_available_node_versions(self, key: Key) -> Tuple[SGNodeAPI, ...]:
        content_key = graph_key_to_content_key(key)
        content_id = content_key_to_node_id(content_key)
        nodes_near_content = await self.network.iterative_lookup(content_id)
        queried: Set[NodeID] = set()

        send_channel, receive_channel = trio.open_memory_channel[SGNodeAPI](0)

        async def do_get_graph_node(location: Node):
            with trio.move_on_after(GET_GRAPH_NODE_TIMEOUT):
                try:
                    node = self.network.get_graph_node(location, key=key)
                except NotFound:
                    pass
                else:
                    await send_channel.send(node)

        async with receive_channel:
            async with trio.open_nursery() as nursery:
                for node in nodes_near_content:
                    for location in await self.network.locate(node, key=content_key):
                        if location.node_id in queried:
                            continue
                        queried.add(location.node_id)
                        nursery.start_soon(do_get_graph_node, location)
            nodes = tuple([node async for node in receive_channel])

        return nodes

    async def get_node_neighbor_data(self,
                                     node: SGNodeAPI,
                                     ) -> Tuple[NeighborData, ...]:
        return tuple([
            await self.get_available_node_versions(neighbor_key)
            for level, direction, neighbor_key in node.iter_neighbors()
        ])

    async def run(self) -> None:
        cursor: SGNodeAPI
        await self.wait_networked_skip_graph()

        while not self.graph.has_cursor:
            self.logger.debug("Graph has no cursor")
            await trio.sleep(5)

        cursor = self.graph.cursor

        direction = Direction.right

        while self.manager.is_running:
            neighbor_data = await self.get_node_neighbor_data(cursor)
            audit = Node(cursor, neighbor_data)
