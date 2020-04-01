import logging
from typing import Mapping

import trio

from eth_keys import keys

from alexandria._utils import humanize_node_id, public_key_to_node_id
from alexandria.abc import (
    Endpoint,
    MessageAPI,
    PoolAPI,
    SessionAPI,
)
from alexandria.exceptions import SessionNotFound, DuplicateSession
from alexandria.packets import NetworkPacket
from alexandria.session import SessionInitiator, SessionRecipient
from alexandria.typing import NodeID


DEFAULT_LISTEN_ON = Endpoint('0.0.0.0', 8628)


class Pool(PoolAPI):
    logger = logging.getLogger('alexandria.pool.Pool')

    _sessions: Mapping[NodeID, SessionAPI]

    def __init__(self,
                 private_key: keys.PrivateKey,
                 outbound_packet_send_channel: trio.abc.SendChannel[NetworkPacket],
                 inbound_message_send_channel: trio.abc.SendChannel[MessageAPI],
                 ) -> None:
        self._private_key = private_key
        self.public_key = private_key.public_key
        self.local_node_id = public_key_to_node_id(self.public_key)
        self._sessions = {}

        self._outbound_packet_send_channel = outbound_packet_send_channel
        self._inbound_message_send_channel = inbound_message_send_channel

    def get_session(self, remote_node_id: NodeID) -> SessionAPI:
        if remote_node_id not in self._sessions:
            raise SessionNotFound(f"No session found for {humanize_node_id(remote_node_id)}")
        return self._sessions[remote_node_id]

    def create_session(self,
                       remote_node_id: NodeID,
                       remote_endpoint: Endpoint,
                       is_initiator: bool) -> SessionAPI:
        if remote_node_id in self._sessions:
            raise DuplicateSession(f"No session found for {humanize_node_id(remote_node_id)}")

        if is_initiator:
            session = SessionInitiator(
                private_key=self._private_key,
                remote_node_id=remote_node_id,
                remote_endpoint=remote_endpoint,
                outbound_packet_send_channel=self._outbound_packet_send_channel.clone(),
                inbound_message_send_channel=self._inbound_message_send_channel.clone(),
            )
        else:
            session = SessionRecipient(
                private_key=self._private_key,
                remote_node_id=remote_node_id,
                remote_endpoint=remote_endpoint,
                outbound_packet_send_channel=self._outbound_packet_send_channel.clone(),
                inbound_message_send_channel=self._inbound_message_send_channel.clone(),
            )

        # We insert the session here to prevent a race condition where an
        # alternate path to session creation results in a new session being
        # inserted before the `_manage_session` task can run.
        self._sessions[remote_node_id] = session

        return session
