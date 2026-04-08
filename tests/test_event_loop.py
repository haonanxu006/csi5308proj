import unittest

from raft.sim.event_loop import EventLoop


class EventLoopTests(unittest.TestCase):
    def test_runs_events_in_time_order(self) -> None:
        loop = EventLoop()
        seen = []

        loop.add(5, seen.append, "late")
        loop.add(1, seen.append, "early")

        loop.run(until=10)

        self.assertEqual(seen, ["early", "late"])
