import random

# from raft.experiments.metrics import Metrics
from raft.core.node import Node
from raft.core.roles import NodeRole
from raft.core.timers import TimeoutPolicy
from raft.sim.event_loop import EventLoop
from raft.sim.network import Network
from raft.experiments.metrics import Metrics


class Cluster:
    def __init__(
        self,
        size: int,
        timeout_policy: TimeoutPolicy,
        heartbeat_interval: int = 30,
        network_delay_range: tuple[int, int] = (5, 10),
        drop_rate: float = 0.0,
        seed: int | None = None,
    ) -> None:
        self.random = random.Random(seed)
        self.event_loop = EventLoop()
        self.metrics = Metrics()
        cluster_timeout_policy = TimeoutPolicy(
            timeout_policy.min_election_timeout,
            timeout_policy.max_election_timeout,
            rng=self.random if timeout_policy.rng is None else timeout_policy.rng,
        )
        self.network = Network(
            self.event_loop,
            delay_fn=lambda: self.random.randint(*network_delay_range),
            drop_fn=lambda: self.random.random() < drop_rate,
        )
        self.nodes: list[Node] = []

        node_ids = list(range(size))
        for node_id in node_ids:
            node = Node(
                node_id=node_id,
                neighbor_ids=[neighbor for neighbor in node_ids if neighbor != node_id],
                event_loop=self.event_loop,
                network=self.network,
                metrics=self.metrics,
                timeout=cluster_timeout_policy,
                heartbeat_interval=heartbeat_interval,
            )
            self.network.add(node)
            self.nodes.append(node)

    def start(self) -> None:
        for node in self.nodes:
            node.start()

    def run(self, until: int) -> None:
        self.event_loop.run(until)

    def crash_node(self, node_id: int) -> None:
        self.nodes[node_id].crash()

    def leaders(self):
        return [
            node for node in self.nodes if node.active and node.role == NodeRole.LEADER
        ]
