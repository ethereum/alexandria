import logging

from async_service import Service
from eth_utils import humanize_hash
import trio

from alexandria.abc import ConnectionAPI, Endpoint, PacketAPI, SessionAPI, MessageAPI


class Connection(Service, ConnectionAPI):
    logger = logging.getLogger('alexandria.Connection')

    def __init__(self,
                 session: SessionAPI,
                 remote_endpoint: Endpoint,
                 inbound_packet_send_channel: trio.abc.SendChannel[PacketAPI],
                 inbound_packet_receive_channel: trio.abc.ReceiveChannel[PacketAPI],
                 outbound_packet_send_channel: trio.abc.SendChannel[PacketAPI],
                 outbound_packet_receive_channel: trio.abc.ReceiveChannel[PacketAPI]) -> None:
        self.session = session
        self.remote_endpoint = remote_endpoint

        self.inbound_packet_send_channel = inbound_packet_send_channel
        self._inbound_packet_receive_channel = inbound_packet_receive_channel

        self._outbound_packet_send_channel = outbound_packet_send_channel
        self.outbound_packet_receive_channel = outbound_packet_receive_channel
        self._ready = trio.Event()

    async def wait_ready(self) -> None:
        await self._ready.wait()

    async def run(self) -> None:
        self.logger.info(
            'Running connection: %s@%s:%s',
            humanize_hash(self.session.remote_node_id.to_bytes(32, 'big')),
            self.remote_endpoint[0],
            self.remote_endpoint[1],
        )
        async with self._outbound_packet_send_channel:
            self._ready.set()
            self.manager.run_daemon_task(
                self._handle_inbound_packets,
                self._inbound_packet_receive_channel,
            )
            await self.manager.wait_finished()

    async def _handle_inbound_packets(self,
                                      receive_channel: trio.abc.ReceiveChannel[PacketAPI]) -> None:
        async with receive_channel:
            async for packet in receive_channel:
                result = await self.session.handle_inbound_packet(packet)
                if isinstance(result, PacketAPI):
                    await self._outbound_packet_send_channel.send(result)

    async def send_packet(self, packet: PacketAPI) -> None:
        await self._outbound_packet_send_channel.send(
            await self.session.handle_outbound_packet(packet)
        )

    async def send_message(self, message: MessageAPI) -> None:
        raise NotImplementedError
