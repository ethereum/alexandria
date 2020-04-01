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
            endpoint = Endpoint(ip_address, port)
            inbound_datagram = Datagram(data, endpoint)
            logger.debug('handling inbound datagram: %s', inbound_datagram)
            try:
                await send_channel.send(inbound_datagram)
            except trio.BrokenResourceError:
                break


async def _handle_outbound(socket: trio.socket.SocketType,
                           receive_channel: trio.abc.SendChannel[Datagram],
                           ) -> None:
    async with receive_channel:
        async for datagram, endpoint in receive_channel:
            logger.debug('handling outbound datagram: %s', (datagram, endpoint))
            await socket.sendto(datagram, endpoint)


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
        logger.debug('Datagram listener started on %s:%s', ip_address, port)
        nursery.start_soon(_handle_inbound, socket, inbound_datagram_send_channel)
        nursery.start_soon(_handle_outbound, socket, outbound_datagram_receive_channel)
        yield
