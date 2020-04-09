import ipaddress
import logging

from async_service import Service

import trio

from alexandria.abc import (
    Datagram,
    Endpoint,
    EventsAPI,
)
from alexandria.constants import DATAGRAM_BUFFER_SIZE


logger = logging.getLogger('alexandria.datagrams')


class DatagramListener(Service):
    def __init__(self,
                 listen_on: Endpoint,
                 inbound_datagram_send_channel: trio.abc.SendChannel[Datagram],
                 outbound_datagram_receive_channel: trio.abc.ReceiveChannel[Datagram],
                 events: EventsAPI,
                 ) -> None:
        self._listen_on = listen_on
        self._inbound_datagram_send_channel = inbound_datagram_send_channel
        self._outbound_datagram_receive_channel = outbound_datagram_receive_channel

        self._listening = trio.Event()
        self._events = events

    async def wait_listening(self) -> None:
        await self._listening.wait()

    async def run(self) -> None:
        socket = trio.socket.socket(
            family=trio.socket.AF_INET,
            type=trio.socket.SOCK_DGRAM,
        )
        ip_address, port = self._listen_on
        await socket.bind((str(ip_address), port))

        self._listening.set()

        logger.debug('Network connection listening on %s', self._listen_on)
        self.manager.run_daemon_task(
            self._handle_inbound,
            socket,
            self._inbound_datagram_send_channel,
        )
        self.manager.run_daemon_task(
            self._handle_outbound,
            socket,
            self._outbound_datagram_receive_channel,
        )

        await self.manager.wait_finished()

    async def _handle_inbound(self,
                              socket: trio.socket.SocketType,
                              send_channel: trio.abc.SendChannel[Datagram]) -> None:
        async with send_channel:
            while True:
                data, (ip_address, port) = await socket.recvfrom(DATAGRAM_BUFFER_SIZE)
                endpoint = Endpoint(ipaddress.IPv4Address(ip_address), port)
                datagram = Datagram(data, endpoint)
                logger.debug('inbound datagram: %s', datagram)
                try:
                    await send_channel.send(datagram)
                    await self._events.datagram_received.trigger(datagram)
                except trio.BrokenResourceError:
                    break

    async def _handle_outbound(self,
                               socket: trio.socket.SocketType,
                               receive_channel: trio.abc.ReceiveChannel[Datagram],
                               ) -> None:
        async with receive_channel:
            async for datagram in receive_channel:
                logger.debug('outbound datagram: %s', datagram)
                data, endpoint = datagram
                await socket.sendto(data, (str(endpoint.ip_address), endpoint.port))
                await self._events.datagram_sent.trigger(datagram)
