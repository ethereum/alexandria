import logging
from typing import Dict

from eth_keys import keys
from ssz import sedes
import trio

from alexandria._utils import humanize_node_id, public_key_to_node_id
from alexandria.abc import (
    EventsAPI,
    MessageAPI,
    NetworkPacket,
    Node,
    PoolAPI,
    SessionAPI,
)
from alexandria.exceptions import SessionNotFound, DuplicateSession
from alexandria.session import SessionInitiator, SessionRecipient
from alexandria.typing import NodeID


class Pool(PoolAPI):
    logger = logging.getLogger('alexandria.pool.Pool')

    _sessions: Dict[NodeID, SessionAPI]

    def __init__(self,
                 private_key: keys.PrivateKey,
                 events: EventsAPI,
                 outbound_packet_send_channel: trio.abc.SendChannel[NetworkPacket],
                 inbound_message_send_channel: trio.abc.SendChannel[MessageAPI[sedes.Serializable]],
                 ) -> None:
        self._private_key = private_key
        self.public_key = private_key.public_key
        self.local_node_id = public_key_to_node_id(self.public_key)
        self._sessions = {}

        self._events = events
        self._outbound_packet_send_channel = outbound_packet_send_channel
        self._inbound_message_send_channel = inbound_message_send_channel

    def remove_session(self, remote_node_id: NodeID) -> None:
        if remote_node_id in self._sessions:
            self._sessions.pop(remote_node_id)

    def has_session(self, remote_node_id: NodeID) -> bool:
        return remote_node_id in self._sessions

    def get_session(self, remote_node_id: NodeID) -> SessionAPI:
        if remote_node_id not in self._sessions:
            raise SessionNotFound(f"No session found for {humanize_node_id(remote_node_id)}")
        return self._sessions[remote_node_id]

    def create_session(self,
                       remote_node: Node,
                       is_initiator: bool) -> SessionAPI:
        if remote_node.node_id in self._sessions:
            raise DuplicateSession(
                f"Session already present for {humanize_node_id(remote_node.node_id)}"
            )

        session: SessionAPI
        if is_initiator:
            session = SessionInitiator(
                private_key=self._private_key,
                remote_node=remote_node,
                events=self._events,
                outbound_packet_send_channel=self._outbound_packet_send_channel.clone(),  # type: ignore  # noqa: E501
                inbound_message_send_channel=self._inbound_message_send_channel.clone(),  # type: ignore  # noqa: E501
            )
        else:
            session = SessionRecipient(
                private_key=self._private_key,
                remote_node=remote_node,
                events=self._events,
                outbound_packet_send_channel=self._outbound_packet_send_channel.clone(),  # type: ignore  # noqa: E501
                inbound_message_send_channel=self._inbound_message_send_channel.clone(),  # type: ignore  # noqa: E501
            )

        self._sessions[remote_node.node_id] = session

        return session
