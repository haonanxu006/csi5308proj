from raft.core.messages import Message, MessageType
from raft.core.timers import TimeoutPolicy
from raft.core.roles import NodeRole
from raft.sim.event_loop import EventLoop, Event
from raft.sim.network import Network
from raft.experiments.metrics import Metrics


class Node:
    node_id: int
    neighbor_ids: list[int]  # complete graph
    event_loop: EventLoop
    network: Network
    timeout: TimeoutPolicy
    heartbeat_interval: int  # heartbeat interval if leader
    metrics: Metrics

    role: NodeRole
    current_term: int
    voted_for: int  # node id of the node voted for
    active: bool

    _election_event: Event  # current election event
    _heartbeat_event: Event
    _votes_received: set[int]

    def __init__(
        self,
        node_id: int,
        neighbor_ids: list[int],
        event_loop: EventLoop,
        network: Network,
        timeout: TimeoutPolicy,
        heartbeat_interval: int,
        metrics: Metrics,
    ) -> None:
        self.node_id = node_id
        self.neighbor_ids = neighbor_ids
        self.event_loop = event_loop
        self.network = network
        self.timeout = timeout
        self.heartbeat_interval = heartbeat_interval
        self.metrics = metrics

        self.role = NodeRole.FOLLOWER
        self.current_term = 0
        self.voted_for: int | None = None
        self.active = True

        self._election_event = None
        self._heartbeat_event = None
        self._votes_received = set()

    def start(self) -> None:
        self.reset_election_timer()

    def reset_election_timer(self) -> None:
        """
        Simulate election timeout
        """
        if self._election_event is not None:
            self._election_event.cancel()
        delay = self.timeout.sample()
        self._election_event = self.event_loop.add(delay, self._on_election_timeout)

    def _on_election_timeout(self) -> None:
        """
        restart election if last election timed out
        """
        if not self.active or self.role == NodeRole.LEADER:
            return
        self.start_election()

    def start_election(self) -> None:
        """
        Node starts election: turn candidate, vote for self, request vote to all neighbors
        """
        self.role = NodeRole.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self._votes_received = {self.node_id}
        self.metrics.record_election_started(self.current_term, self.event_loop.time)
        self.reset_election_timer()

        for nid in self.neighbor_ids:
            # construct request vote message and add to eventloop
            m = Message(self.node_id, nid, self.current_term, MessageType.REQUEST_VOTE)
            self.network.send(m)

    def on_message(self, message: Message):
        if not self.active:
            return

        if message.term > self.current_term:
            # if message term is larger, reset node to follower and update term
            self.current_term = message.term
            self.role = NodeRole.FOLLOWER
            self.voted_for = None
            self._votes_received.clear()

        if message.type == MessageType.REQUEST_VOTE:
            self._on_request_vote(message)
            return

        if message.type == MessageType.REQUEST_VOTE_RESPONSE:
            self._on_request_vote_response(message)
            return

        if message.type == MessageType.APPEND_ENTRIES:
            self._on_append_entries(message)
            return

    def _on_request_vote(self, message: Message) -> None:
        granted = False

        if message.term < self.current_term:
            granted = False
        else:
            if self.role == NodeRole.LEADER and message.term == self.current_term:
                granted = False
            elif self.voted_for in (None, message.src):
                self.role = NodeRole.FOLLOWER
                self.voted_for = message.src
                self.reset_election_timer()
                granted = True

        response = Message(
            src=self.node_id,
            dst=message.src,
            term=self.current_term,
            type=MessageType.REQUEST_VOTE_RESPONSE,
            data=(granted,),
        )
        self.network.send(response)

    def _on_request_vote_response(self, message: Message) -> None:
        if self.role != NodeRole.CANDIDATE:
            return

        if message.term != self.current_term:
            return

        if not message.data[0]:
            # vote not granted
            return

        self._votes_received.add(message.src)

        if len(self._votes_received) > (len(self.neighbor_ids) + 1) // 2:
            # receives majority votes
            self._become_leader()

    def _become_leader(self) -> None:
        self.role = NodeRole.LEADER
        self._votes_received.clear()
        if self._election_event is not None:
            self._election_event.cancel()
            self._election_event = None
        election_started_at = self.metrics.first_election_start_for_term(
            self.current_term
        )
        elected_at = self.event_loop.time
        self.metrics.record_leader_elected(
            term=self.current_term,
            leader_id=self.node_id,
            elected_at=elected_at,
            election_duration=(
                0
                if election_started_at is None
                else elected_at - election_started_at
            ),
        )
        self._send_heartbeats()

    def _send_heartbeats(self) -> None:
        if not self.active or self.role != NodeRole.LEADER:
            return

        for nid in self.neighbor_ids:
            # send heartbeat to all neighbors
            m = Message(
                self.node_id, nid, self.current_term, MessageType.APPEND_ENTRIES
            )
            self.network.send(m)

        # heartbeat interval
        self._heartbeat_event = self.event_loop.add(
            self.heartbeat_interval,
            self._send_heartbeats,
        )

    def _on_append_entries(self, message: Message):
        if message.term < self.current_term:
            return

        self.role = NodeRole.FOLLOWER
        self.current_term = message.term
        self.voted_for = None
        self._votes_received.clear()
        self.reset_election_timer()

    def crash(self) -> None:
        """
        Simulate node crash
        """
        self.active = False
        self.role = NodeRole.FOLLOWER
        self._votes_received.clear()

        # cancel all pending events
        if self._election_event is not None:
            self._election_event.cancel()
            self._election_event = None
        if self._heartbeat_event is not None:
            self._heartbeat_event.cancel()
            self._heartbeat_event = None

    def restart(self) -> None:
        """
        Simulate node restart
        """
        self.active = True
        self.role = NodeRole.FOLLOWER
        self.voted_for = None
        self.reset_election_timer()
