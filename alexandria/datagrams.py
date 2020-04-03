import ipaddress
import logging
from typing import (
    AsyncIterable,
)

from async_generator import asynccontextmanager
import trio

from alexandria.abc import (
    Datagram,
    Endpoint,
)
from alexandria.constants import DATAGRAM_BUFFER_SIZE


logger = logging.getLogger('alexandria.datagrams')


async def _handle_inbound(socket: trio.socket.SocketType,
                          send_channel: trio.abc.SendChannel[Datagram]) -> None:
    async with send_channel:
        while True:
            data, (ip_address, port) = await socket.recvfrom(DATAGRAM_BUFFER_SIZE)
            endpoint = Endpoint(ipaddress.IPv4Address(ip_address), port)
            datagram = Datagram(data, endpoint)
            logger.debug('inbound datagram: %s', datagram)
            try:
                await send_channel.send(datagram)
            except trio.BrokenResourceError:
                break


async def _handle_outbound(socket: trio.socket.SocketType,
                           receive_channel: trio.abc.SendChannel[Datagram],
                           ) -> None:
    async with receive_channel:
        async for datagram in receive_channel:
            logger.debug('outbound datagram: %s', datagram)
            data, endpoint = datagram
            await socket.sendto(data, (str(endpoint.ip_address), endpoint.port))


@asynccontextmanager
async def listen(endpoint: Endpoint,
                 inbound_datagram_send_channel: trio.abc.SendChannel[Datagram],
                 outbound_datagram_receive_channel: trio.abc.ReceiveChannel[Datagram],
                 ) -> AsyncIterable[None]:
    socket = trio.socket.socket(
        family=trio.socket.AF_INET,
        type=trio.socket.SOCK_DGRAM,
    )
    ip_address, port = endpoint
    await socket.bind((str(ip_address), port))

    async with trio.open_nursery() as nursery:
        logger.debug('Network connection listening on %s', endpoint)
        nursery.start_soon(_handle_inbound, socket, inbound_datagram_send_channel)
        nursery.start_soon(_handle_outbound, socket, outbound_datagram_receive_channel)
        yield
