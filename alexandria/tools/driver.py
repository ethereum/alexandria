from abc import ABC, abstractmethod
import logging
from typing import Any, Sequence

from async_service import Service


class DriverAPI(ABC):
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


class Instruction(Callable[[], Any]):
    def __init__(self, callback_fn: Callable[[], Any]) -> None:
        self._callback_fn = callback_fn

    def __call__(self) -> Any:
        return self._callback_fn


class DriverState:
    def __init__(self):


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
            self.logger.debug('%s: Executing instruction #{index} - {instruction}', self, index, instruction)
            raise NotImplementedError


async def listen(alice_endpoint):
    pass

async def send_message(Ping(1234)):
    pass

async def wait_for_message(Pong):
    pass
