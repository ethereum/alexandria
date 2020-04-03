import collections
import functools
import itertools
import logging
from typing import Deque, Iterable, Tuple

from alexandria._utils import humanize_node_id
from alexandria.abc import RoutingTableAPI
from alexandria.constants import KEY_BIT_SIZE
from alexandria.typing import NodeID


def compute_distance(left_node_id: NodeID, right_node_id: NodeID) -> int:
    return left_node_id ^ right_node_id


def compute_log_distance(left_node_id: NodeID, right_node_id: NodeID) -> int:
    if left_node_id == right_node_id:
        raise ValueError("Cannot compute log distance between identical nodes")
    distance = compute_distance(left_node_id, right_node_id)
    return distance.bit_length()


class RoutingTable(RoutingTableAPI):
    logger = logging.getLogger("p2p.discv5.routing_table.KademliaRoutingTable")

    def __init__(self, center_node_id: NodeID, bucket_size: int, num_bits=KEY_BIT_SIZE) -> None:
        self.center_node_id = center_node_id
        self.bucket_size = bucket_size
        self._num_bits = num_bits

        self.buckets: Tuple[Deque[NodeID], ...] = tuple(
            collections.deque(maxlen=bucket_size) for _ in range(self._num_bits)
        )
        self.replacement_caches: Tuple[Deque[NodeID], ...] = tuple(
            collections.deque() for _ in range(self._num_bits)
        )

        self.bucket_update_order: Deque[int] = collections.deque()

    def get_index_bucket_and_replacement_cache(self,
                                               node_id: NodeID,
                                               ) -> Tuple[int, Deque[NodeID], Deque[NodeID]]:
        index = compute_log_distance(self.center_node_id, node_id) - 1
        bucket = self.buckets[index]
        replacement_cache = self.replacement_caches[index]
        return index, bucket, replacement_cache

    def update(self, node_id: NodeID) -> NodeID:
        """Insert a node into the routing table or move it to the top if already present.

        If the bucket is already full, the node id will be added to the replacement cache and
        the oldest node is returned as an eviction candidate. Otherwise, the return value is
        `None`.
        """
        if node_id == self.center_node_id:
            raise ValueError("Cannot insert center node into routing table")

        bucket_index, bucket, replacement_cache = self.get_index_bucket_and_replacement_cache(
            node_id,
        )

        is_bucket_full = len(bucket) >= self.bucket_size
        is_node_in_bucket = node_id in bucket

        if not is_node_in_bucket and not is_bucket_full:
            self.logger.debug("Adding %s to bucket %d", humanize_node_id(node_id), bucket_index)
            self.update_bucket_unchecked(node_id)
            eviction_candidate = None
        elif is_node_in_bucket:
            self.logger.debug("Updating %s in bucket %d", humanize_node_id(node_id), bucket_index)
            self.update_bucket_unchecked(node_id)
            eviction_candidate = None
        elif not is_node_in_bucket and is_bucket_full:
            if node_id not in replacement_cache:
                self.logger.debug(
                    "Adding %s to replacement cache of bucket %d",
                    humanize_node_id(node_id),
                    bucket_index,
                )
            else:
                self.logger.debug(
                    "Updating %s in replacement cache of bucket %d",
                    humanize_node_id(node_id),
                    bucket_index,
                )
                replacement_cache.remove(node_id)
            replacement_cache.appendleft(node_id)
            eviction_candidate = bucket[-1]
        else:
            raise Exception("unreachable")

        return eviction_candidate

    def update_bucket_unchecked(self, node_id: NodeID) -> None:
        """Add or update assuming the node is either present already or the bucket is not full."""
        bucket_index, bucket, replacement_cache = self.get_index_bucket_and_replacement_cache(
            node_id,
        )

        for container in (bucket, replacement_cache):
            try:
                container.remove(node_id)
            except ValueError:
                pass
        bucket.appendleft(node_id)

        try:
            self.bucket_update_order.remove(bucket_index)
        except ValueError:
            pass
        self.bucket_update_order.appendleft(bucket_index)

    def remove(self, node_id: NodeID) -> None:
        """Remove a node from the routing table if it is present.

        If possible, the node will be replaced with the newest entry in the replacement cache.
        """
        bucket_index, bucket, replacement_cache = self.get_index_bucket_and_replacement_cache(
            node_id,
        )

        in_bucket = node_id in bucket
        in_replacement_cache = node_id in replacement_cache

        if in_bucket:
            bucket.remove(node_id)
            if replacement_cache:
                replacement_node_id = replacement_cache.popleft()
                self.logger.debug(
                    "Replacing %s from bucket %d with %s from replacement cache",
                    humanize_node_id(node_id),
                    bucket_index,
                    humanize_node_id(replacement_node_id),
                )
                bucket.append(replacement_node_id)
            else:
                self.logger.debug(
                    "Removing %s from bucket %d without replacement",
                    humanize_node_id(node_id),
                    bucket_index,
                )

        if in_replacement_cache:
            self.logger.debug(
                "Removing %s from replacement cache of bucket %d",
                humanize_node_id(node_id),
                bucket_index,
            )
            replacement_cache.remove(node_id)

        if not in_bucket and not in_replacement_cache:
            self.logger.debug(
                "Not removing %s as it is neither present in the bucket nor the replacement cache",
                humanize_node_id(node_id),
                bucket_index,
            )

        # bucket_update_order should only contain non-empty buckets, so remove it if necessary
        if not bucket:
            try:
                self.bucket_update_order.remove(bucket_index)
            except ValueError:
                pass

    def get_nodes_at_log_distance(self, log_distance: int) -> Tuple[NodeID, ...]:
        """Get all nodes in the routing table at the given log distance to the center."""
        if log_distance <= 0:
            raise ValueError(f"Log distance must be positive, got {log_distance}")
        elif log_distance > len(self.buckets):
            raise ValueError(
                f"Log distance must not be greater than {len(self.buckets)}, got {log_distance}"
            )
        return tuple(self.buckets[log_distance - 1])

    @property
    def is_empty(self) -> bool:
        return all(len(bucket) == 0 for bucket in self.buckets)

    def get_least_recently_updated_log_distance(self) -> int:
        """Get the log distance whose corresponding bucket was updated least recently.

        Only non-empty buckets are considered. If all buckets are empty, a `ValueError` is raised.
        """
        try:
            bucket_index = self.bucket_update_order[-1]
        except IndexError:
            raise ValueError("Routing table is empty")
        else:
            return bucket_index + 1

    def iter_nodes_around(self, reference_node_id: NodeID) -> Iterable[NodeID]:
        """Iterate over all nodes in the routing table ordered by distance to a given reference."""
        all_node_ids = itertools.chain(*self.buckets)
        distance_to_reference = functools.partial(compute_distance, reference_node_id)
        sorted_node_ids = sorted(all_node_ids, key=distance_to_reference)
        for node_id in sorted_node_ids:
            yield node_id

    def iter_nodes(self) -> Iterable[NodeID]:
        yield from itertools.chain(*self.buckets)
