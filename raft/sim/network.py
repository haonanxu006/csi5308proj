from raft.sim.event_loop import EventLoop
from raft.core.messages import Message
from typing import Callable


class Network:
    """
    Simple wrapper around eventloop add() to simulate network delay.
    Only the 3 main types of messages are handled through network.
    """

    event_loop: EventLoop
    nodes: dict[int, object]
    delay_fn: Callable[[], int]  # simulate network delay
    drop_fn: Callable[[], bool]  # simulate packet loss
    message_count: int
    dropped_count: int

    def __init__(
        self,
        event_loop: EventLoop,
        delay_fn: Callable[[], int],
        drop_fn: Callable[[], bool],
    ) -> None:
        self.event_loop = event_loop
        self.nodes = {}
        self.delay_fn = delay_fn
        self.drop_fn = drop_fn
        self.message_count = 0
        self.dropped_count = 0

    def add(self, node: object) -> None:
        self.nodes[node.node_id] = node

    def send(self, message: Message) -> None:
        self.message_count += 1

        if self.drop_fn():
            self.dropped_count += 1
            return

        delay = self.delay_fn()

        node = self.nodes.get(message.dst)
        if node is not None:
            # callback handled all by on message (for 3 main types)
            self.event_loop.add(delay, node.on_message, message)
