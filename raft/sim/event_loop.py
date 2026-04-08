from dataclasses import dataclass, field
from typing import Any, Callable
import heapq


@dataclass(order=True)
class Event:
    """
    Compare time to order
    """

    time: int
    _order: int  # tie breaker for ranking event
    callback: Callable[..., None] = field(compare=False)
    args: tuple[Any, ...] = field(compare=False, default_factory=tuple)
    cancelled: bool = field(compare=False, default=False)

    def cancel(self) -> None:
        self.cancelled = True


class EventLoop:
    """
    Simple event loop to simulate real event delivery to avoid actual waiting
    """

    time: int
    _queue: list[Event]
    _counter: int  # for order in event

    def __init__(self):
        self.time = 0
        self._queue = []
        self._counter = 0

    def add(self, delay: int, callback: Callable[..., None], *args: Any) -> Event:
        if delay < 0:
            raise ValueError("invalid delay; must be positive")
        event = Event(self.time + delay, self._counter, callback, args)
        heapq.heappush(self._queue, event)  # maintain a min heap
        self._counter += 1
        return event

    def run(self, until: int | None) -> None:
        while self._queue:
            event = heapq.heappop(self._queue)  # get the first event to execute
            if event.cancelled:
                continue
            if until is not None and event.time > until:
                # max duration is reached -> requeue and stop simulation
                heapq.heappush(self._queue, event)
                break
            self.time = event.time  # increment simulated time
            event.callback(*event.args)  # execute event
