import collections
import logging
import secrets
from typing import (
    DefaultDict,
    Set,
    Tuple,
    Type,
)

from async_service import Service
from ssz import sedes

import trio

from alexandria.abc import MessageAPI, NodeID, RegistryAPI, SubscriptionAPI, TPayload
from alexandria.messages import default_registry
from alexandria.subscriptions import Subscription


def get_random_request_id():
    return secrets.randbits(16)


MAX_REQUEST_ID_ATTEMPTS = 3


class MessageDispatcher(Service):
    logger = logging.getLogger('alexandria.subscriptions.SubscriptionManager')

    _subscriptions: DefaultDict[int, Set[trio.abc.SendChannel[MessageAPI[sedes.Serializable]]]]
    _reserved_request_ids: Set[Tuple[NodeID, int]]

    def __init__(self,
                 outbound_message_send_channel: trio.abc.SendChannel[MessageAPI[sedes.Serializable]],  # noqa: E501
                 inbound_message_receive_channel: trio.abc.ReceiveChannel[MessageAPI[sedes.Serializable]],  # noqa: E501
                 message_registry: RegistryAPI = default_registry,
                 ) -> None:
        self._registry = message_registry
        self._inbound_message_receive_channel = inbound_message_receive_channel
        self._outbound_message_send_channel = outbound_message_send_channel
        self._subscriptions = collections.defaultdict(set)
        self._reserved_request_ids = set()

    async def run(self) -> None:
        self.manager.run_daemon_task(
            self._handle_inbound_messages,
            self._inbound_message_receive_channel,
        )
        await self.manager.wait_finished()

    async def _handle_inbound_messages(self,
                                       receive_channel: trio.abc.ReceiveChannel[MessageAPI[sedes.Serializable]],  # noqa: E501
                                       ) -> None:
        try:
            async with trio.open_nursery() as nursery:
                async with receive_channel:
                    async for message in receive_channel:
                        #
                        # Subscriptions
                        #
                        channels = self._subscriptions[message.message_id]
                        self.logger.debug(
                            'Handling %d subscriptions for message: %s',
                            len(channels),
                            message,
                        )
                        for send_channel in channels:
                            nursery.start_soon(send_channel.send, message)
        except trio.BrokenResourceError:
            pass

    #
    # Utility
    #
    def get_free_request_id(self, node_id: NodeID) -> int:
        for _ in range(MAX_REQUEST_ID_ATTEMPTS):
            request_id = get_random_request_id()
            if (node_id, request_id) not in self._reserved_request_ids:
                return request_id
        else:
            # this should be extremely unlikely to happen
            raise ValueError(
                f"Failed to get free request id ({len(self.response_handler_send_channels)} "
                f"handlers added right now)"
            )

    #
    # Message Sending
    #
    async def send_message(self, message: MessageAPI[sedes.Serializable]) -> None:
        await self._outbound_message_send_channel.send(message)

    #
    # Request Response
    #
    def subscribe(self, payload_type: Type[TPayload]) -> SubscriptionAPI[MessageAPI[TPayload]]:
        message_id = self._registry.get_message_id(payload_type)
        send_channel, receive_channel = trio.open_memory_channel[MessageAPI[TPayload]](256)
        self._subscriptions[message_id].add(send_channel)
        subscription = Subscription(
            lambda: self._subscriptions[message_id].remove(send_channel),
            receive_channel,
        )
        return subscription

    def subscribe_request(self,
                          request: MessageAPI[sedes.Serializable],
                          response_payload_type: Type[TPayload],
                          ) -> SubscriptionAPI[MessageAPI[TPayload]]:
        node_id = request.node.node_id
        request_id = request.payload.request_id

        self.logger.debug(
            "Sending request: %s with request id %d",
            request,
            request_id,
        )

        send_channel, receive_channel = trio.open_memory_channel[MessageAPI[TPayload]](256)
        key = (node_id, request_id)
        self._reserved_request_ids.add(key)

        subscription = Subscription(
            lambda: self._reserved_request_ids.remove(key),
            receive_channel,
        )
        self.manager.run_task(
            self._manage_request_response,
            request,
            response_payload_type,
            send_channel,
        )
        return subscription

    async def _manage_request_response(self,
                                       request: MessageAPI[sedes.Serializable],
                                       response_payload_type: Type[sedes.Serializable],
                                       send_channel: trio.abc.SendChannel[MessageAPI[sedes.Serializable]],  # noqa: E501
                                       ) -> None:
        node_id = request.node.node_id
        request_id = request.payload.request_id

        with self.subscribe(response_payload_type) as subscription:
            self.logger.debug(
                "Sending request with request id %d",
                request_id,
            )
            # Send the request
            await self.send_message(request)

            # Wait for the response
            async with send_channel:
                async for response in subscription.stream():
                    if response.node.node_id != node_id or response.payload.request_id != request_id:  # noqa: E501
                        continue
                    else:
                        await send_channel.send(response)
