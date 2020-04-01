from abc import ABC
import logging
from typing import Any, Awaitable, Callable, Sequence

import trio
from async_service import Service

from alexandria.abc import Endpoint, Datagram, PacketAPI, MessageAPI
from alexandria.datagram import listen as datagram_listen


class DriverAPI(ABC):
    instruction_nursery: trio.Nursery

    outbound_datagram_send_channel: trio.abc.SendChannel[Datagram]
    outbound_datagram_receive_channel: trio.abc.ReceiveChannel[Datagram]
    inbound_datagram_send_channel: trio.abc.SendChannel[Datagram]
    inbound_datagram_receive_channel: trio.abc.ReceiveChannel[Datagram]

    outbound_packet_send_channel: trio.abc.SendChannel[PacketAPI]
    outbound_packet_receive_channel: trio.abc.ReceiveChannel[PacketAPI]
    inbound_packet_send_channel: trio.abc.SendChannel[PacketAPI]
    inbound_packet_receive_channel: trio.abc.ReceiveChannel[PacketAPI]

    outbound_message_send_channel: trio.abc.SendChannel[MessageAPI]
    outbound_message_receive_channel: trio.abc.ReceiveChannel[MessageAPI]
    inbound_message_send_channel: trio.abc.SendChannel[MessageAPI]
    inbound_message_receive_channel: trio.abc.ReceiveChannel[MessageAPI]


Instruction = Callable[[DriverAPI], Awaitable[Any]]


class Driver(Service):
    logger = logging.getLogger('alexandria.tools.driver.Driver')

    def __init__(self, instructions: Sequence[Instruction], name: str = None) -> None:
        self._name = name
        self._instructions = instructions

        # Datagrams
        (
            self.outbound_datagram_send_channel,
            self.outbound_datagram_receive_channel,
        ) = trio.open_memory_channel[Datagram](0)
        (
            self.inbound_datagram_send_channel,
            self.inbound_datagram_receive_channel,
        ) = trio.open_memory_channel[Datagram](0)

        # Packets
        (
            self.outbound_packet_send_channel,
            self.outbound_packet_receive_channel,
        ) = trio.open_memory_channel[PacketAPI](0)
        (
            self.inbound_packet_send_channel,
            self.inbound_packet_receive_channel,
        ) = trio.open_memory_channel[PacketAPI](0)

        # Messages
        (
            self.outbound_message_send_channel,
            self.outbound_message_receive_channel,
        ) = trio.open_memory_channel[MessageAPI](0)
        (
            self.inbound_message_send_channel,
            self.inbound_message_receive_channel,
        ) = trio.open_memory_channel[MessageAPI](0)

    def __str__(self) -> str:
        return f"Driver[{self._name}]"

    async def run(self):

        self.logger.info('%s: Starting with %d instructions.', self._name, len(self._instructions))
        for index, instruction in enumerate(self._instructions):
            self.logger.debug(
                '%s: Executing instruction #{index} - {instruction}',
                self,
                index,
                instruction,
            )
            raise NotImplementedError


def listen(alice_endpoint: Endpoint):
    async def listen_instruction(driver: DriverAPI):
        listening = trio.Event()
        manager = driver.get_manager()

        async def _do_listen():
            listener = datagram_listen(
                alice_endpoint,
                driver.inbound_datagram_send_channel,
                driver.outbound_datagram_receive_channel,
            )
            async with listener:
                listening.set()
                await manager.wait_finished()
        driver.instruction_nursery.start_soon(_do_listen)
        await listening.wait()
    return listen_instruction


def send_message(message: MessageAPI):
    async def send_message_instruction(driver: DriverAPI):
        raise NotImplementedError
    return send_message_instruction


def wait_for_message(Pong):
    async def wait_for_message_instruction(driver: DriverAPI):
        raise NotImplementedError
    return wait_for_message_instruction
