import collections
import io
import ipaddress
import json
import logging
import pathlib
import time
from typing import Any, List, Mapping, Optional, Tuple, cast

from eth_utils import ValidationError, to_int, decode_hex, encode_hex
from mypy_extensions import TypedDict
import trio

from async_service import Service

from alexandria._utils import node_id_to_hex
from alexandria.abc import (
    ClientAPI,
    ContentBundle,
    Endpoint,
    KademliaAPI,
    NetworkAPI,
    Node,
    RoutingTableAPI,
)
from alexandria.constants import (
    ADVERTISE_TIMEOUT,
    LOCATE_TIMEOUT,
    PING_TIMEOUT,
    RETRIEVE_TIMEOUT,
)
from alexandria.typing import NodeID

NEW_LINE = "\n"


def strip_non_json_prefix(raw_request: str) -> Tuple[str, str]:
    if raw_request and raw_request[0] != '{':
        prefix, bracket, rest = raw_request.partition('{')
        return prefix.strip(), bracket + rest
    else:
        return '', raw_request


async def write_error(socket: trio.socket.SocketType, message: str) -> None:
    json_error = json.dumps({'error': message})
    await socket.send(json_error.encode('utf8'))


def validate_request(request: Mapping[Any, Any]) -> None:
    try:
        version = request['jsonrpc']
    except KeyError as err:
        raise ValidationError("Missing 'jsonrpc' key") from err
    else:
        if version != '2.0':
            raise ValidationError(f"Invalid version: {version}")

    if 'method' not in request:
        raise ValidationError("Missing 'method' key")
    if 'params' in request:
        if not isinstance(request['params'], list):
            raise ValidationError("Missing 'method' key")


class RPCRequest(TypedDict):
    jsonrpc: str
    method: str
    params: List[Any]
    id: Optional[int]


def generate_response(request: RPCRequest, result: Any, error: Optional[str]) -> str:
    response = {
        'id': request.get('id', -1),
        'jsonrpc': request.get('jsonrpc', "2.0"),
    }

    if result is None and error is None:
        raise ValueError("Must supply either result or error for JSON-RPC response")
    if result is not None and error is not None:
        raise ValueError("Must not supply both a result and an error for JSON-RPC response")
    elif result is not None:
        response['result'] = result
    elif error is not None:
        response['error'] = str(error)
    else:
        raise Exception("Unreachable code path")

    return json.dumps(response)


def node_to_rpc(node: Node) -> Tuple[str, str, int]:
    return (
        node_id_to_hex(node.node_id),
        str(node.endpoint.ip_address),
        node.endpoint.port,
    )


def node_from_rpc(rpc_node: Tuple[str, str, int]) -> Node:
    node_id_as_hex, ip_address, port = rpc_node
    node_id = NodeID(to_int(hexstr=node_id_as_hex))
    node = Node(
        node_id,
        Endpoint(ipaddress.IPv4Address(ip_address), port),
    )
    return node


def node_id_from_hex(node_id_as_hex: str) -> NodeID:
    return NodeID(to_int(hexstr=node_id_as_hex))


class RPCServer(Service):
    logger = logging.getLogger('alexandria.rpc.RPCServer')

    def __init__(self,
                 ipc_path: pathlib.Path,
                 client: ClientAPI,
                 network: NetworkAPI,
                 kademlia: KademliaAPI,
                 routing_table: RoutingTableAPI) -> None:
        self.ipc_path = ipc_path
        self.client = client
        self.network = network
        self.kademlia = kademlia
        self.routing_table = routing_table
        self._serving = trio.Event()

    async def wait_serving(self) -> None:
        await self._serving.wait()

    async def run(self) -> None:
        self.manager.run_daemon_task(self.serve, self.ipc_path)
        try:
            await self.manager.wait_finished()
        finally:
            self.ipc_path.unlink()

    async def execute_rpc(self, request: RPCRequest) -> str:
        namespaced_method = request['method']
        params = request.get('params', [])

        self.logger.debug('RPCServer handling request: %s', namespaced_method)

        namespace, _, method = namespaced_method.partition('_')
        if namespace != 'alexandria':
            return generate_response(request, None, f"Invalid namespace: {namespaced_method}")

        if method == "nodeId":
            return await self._handle_nodeId(request)
        elif method == "routingTableStats":
            return await self._handle_routingTableStats(request)
        elif method == "routingTableBucketInfo":
            return await self._handle_routingTableBucketInfo(request, *params)
        elif method == "ping":
            return await self._handle_ping(request, *params)
        elif method == "findNodes":
            return await self._handle_find_nodes(request, *params)
        elif method == "advertise":
            return await self._handle_advertise(request, *params)
        elif method == "locate":
            return await self._handle_locate(request, *params)
        elif method == "retrieve":
            return await self._handle_retrieve(request, *params)
        elif method == "contentStats":
            return await self._handle_contentStats(request, *params)
        elif method == "addContent":
            return await self._handle_addContent(request, *params)
        else:
            return generate_response(request, None, f"Unknown method: {namespaced_method}")

    async def serve(self, ipc_path: pathlib.Path) -> None:
        self.logger.info("Starting RPC server over IPC socket: %s", ipc_path)

        with trio.socket.socket(trio.socket.AF_UNIX, trio.socket.SOCK_STREAM) as sock:
            # TODO: unclear if the following stuff is necessary:
            # ###################################################
            # These options help fix an issue with the socket reporting itself
            # already being used since it accepts many client connection.
            # https://stackoverflow.com/questions/6380057/python-binding-socket-address-already-in-use
            # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # ###################################################
            await sock.bind(str(ipc_path))
            # Allow up to 10 pending connections.
            sock.listen(10)

            self._serving.set()

            while self.manager.is_running:
                conn, addr = await sock.accept()
                self.logger.debug('Server accepted connection: %r', addr)
                self.manager.run_task(self._handle_connection, conn)

    async def _handle_connection(self, socket: trio.socket.SocketType) -> None:
        buffer = io.StringIO()
        decoder = json.JSONDecoder()

        with socket:
            while True:
                data = await socket.recv(1024)
                buffer.write(data.decode())

                bad_prefix, raw_request = strip_non_json_prefix(buffer.getvalue())
                if bad_prefix:
                    self.logger.info("Client started request with non json data: %r", bad_prefix)
                    await write_error(socket, f'Cannot parse json: {bad_prefix}')
                    continue

                try:
                    request, offset = decoder.raw_decode(raw_request)
                except json.JSONDecodeError:
                    # invalid json request, keep reading data until a valid json is formed
                    if raw_request:
                        self.logger.debug(
                            "Invalid JSON, waiting for rest of message: %r",
                            raw_request,
                        )
                    else:
                        await trio.sleep(0.01)
                    continue

                if not isinstance(request, collections.Mapping):
                    self.logger.debug("Invalid payload: %s", type(request))
                    return

                # TODO: more efficient algorithm can be used here by
                # manipulating the buffer such that we can seek back to the
                # correct position for *new* data to come in.
                buffer.seek(0)
                buffer.write(raw_request[offset:])
                buffer.truncate()

                if not request:
                    self.logger.debug("Client sent empty request")
                    await write_error(socket, 'Invalid Request: empty')
                    continue

                try:
                    validate_request(request)
                except ValidationError as err:
                    await write_error(socket, str(err))
                    continue

                try:
                    result = await self.execute_rpc(cast(RPCRequest, request))
                except Exception as e:
                    self.logger.exception("Unrecognized exception while executing RPC")
                    await write_error(socket, "unknown failure: " + str(e))
                else:
                    if not result.endswith(NEW_LINE):
                        result += NEW_LINE

                    try:
                        await socket.send(result.encode())
                    except BrokenPipeError:
                        break

    async def _handle_nodeId(self, request: RPCRequest) -> str:
        await trio.hazmat.checkpoint()
        node = self.client.local_node
        result = node_to_rpc(node)
        return generate_response(request, result, None)

    async def _handle_routingTableStats(self, request: RPCRequest) -> str:
        await trio.hazmat.checkpoint()
        stats = self.routing_table.get_stats()
        return generate_response(request, stats, None)

    async def _handle_routingTableBucketInfo(self, request: RPCRequest, bucket_idx: int) -> str:
        await trio.hazmat.checkpoint()
        if bucket_idx >= self.routing_table.bucket_size:
            return generate_response(request, None, f"Invalid bucket index: {bucket_idx}")

        info = self.routing_table.get_bucket_info(bucket_idx)
        return generate_response(request, info, None)

    async def _handle_ping(self,
                           request: RPCRequest,
                           node_id_as_hex: str,
                           ip_address: str,
                           port: int) -> str:
        node = node_from_rpc((node_id_as_hex, ip_address, port))
        try:
            with trio.fail_after(PING_TIMEOUT):
                start_at = time.monotonic()
                await self.client.ping(node)
            end_at = time.monotonic()
            return generate_response(request, end_at - start_at, None)
        except trio.TooSlowError:
            return generate_response(
                request,
                None,
                f"Timed out waiting for Pong response from {node}",
            )

    async def _handle_find_nodes(self,
                                 request: RPCRequest,
                                 node_id_as_hex: str,
                                 ip_address: str,
                                 port: int,
                                 distance: int,
                                 ) -> str:
        node = node_from_rpc((node_id_as_hex, ip_address, port))
        if distance < 0 or distance > 256:
            return generate_response(
                request,
                None,
                f"`distance` must be in range [0, 256]: Got {distance}",
            )

        try:
            with trio.fail_after(PING_TIMEOUT):
                found_nodes = await self.network.single_lookup(node, distance=distance)
            payload = tuple(
                node_to_rpc(node)
                for node in found_nodes
            )
            return generate_response(request, payload, None)
        except trio.TooSlowError:
            return generate_response(
                request,
                None,
                f"Timed out waiting for FoundNodes response from {node}",
            )

    async def _handle_advertise(self,
                                request: RPCRequest,
                                node_id_as_hex: str,
                                node_ip_address: str,
                                node_port: int,
                                key_as_hex: str,
                                who_id_as_hex: str,
                                who_ip_address: str,
                                who_port: int,
                                ) -> str:
        key = decode_hex(key_as_hex)
        node = node_from_rpc((node_id_as_hex, node_ip_address, node_port))
        who = node_from_rpc((who_id_as_hex, who_ip_address, who_port))
        try:
            with trio.fail_after(ADVERTISE_TIMEOUT):
                start_at = time.monotonic()
                await self.client.advertise(node, key=key, who=who)
            end_at = time.monotonic()
            return generate_response(request, end_at - start_at, None)
        except trio.TooSlowError:
            return generate_response(
                request,
                None,
                f"Timed out waiting for Ack response from {node}",
            )

    async def _handle_locate(self,
                             request: RPCRequest,
                             node_id_as_hex: str,
                             node_ip_address: str,
                             node_port: int,
                             key_as_hex: str,
                             ) -> str:
        key = decode_hex(key_as_hex)
        node = node_from_rpc((node_id_as_hex, node_ip_address, node_port))
        try:
            with trio.fail_after(LOCATE_TIMEOUT):
                locations = await self.network.locate(node, key=key)
            payload = tuple(node_to_rpc(node) for node in locations)
            return generate_response(request, payload, None)
        except trio.TooSlowError:
            return generate_response(
                request,
                None,
                f"Timed out waiting for Locations response from {node}",
            )

    async def _handle_retrieve(self,
                               request: RPCRequest,
                               node_id_as_hex: str,
                               node_ip_address: str,
                               node_port: int,
                               key_as_hex: str,
                               ) -> str:
        key = decode_hex(key_as_hex)
        node = node_from_rpc((node_id_as_hex, node_ip_address, node_port))
        try:
            with trio.fail_after(RETRIEVE_TIMEOUT):
                data = await self.network.retrieve(node, key=key)
            return generate_response(request, encode_hex(data), None)
        except trio.TooSlowError:
            return generate_response(
                request,
                None,
                f"Timed out waiting for Chunks response from {node}",
            )

    async def _handle_contentStats(self, request: RPCRequest) -> str:
        stats = self.kademlia.content_manager.get_stats()
        return generate_response(request, stats, None)

    async def _handle_addContent(self,
                                 request: RPCRequest,
                                 key_as_hex: str,
                                 data_as_hex: str,
                                 is_ephemeral: bool) -> str:
        key = decode_hex(key_as_hex)
        data = decode_hex(data_as_hex)
        if is_ephemeral:
            bundle = ContentBundle(key, data, self.client.local_node_id)
            self.kademlia.content_manager.ingest_content(bundle)
        else:
            self.kademlia.content_manager.durable_db.set(key, data)
            self.kademlia.content_manager.rebuild_durable_index()
        self.kademlia.advertise_tracker.enqueue(key, last_advertised_at=0.0)
        return generate_response(request, (), None)
