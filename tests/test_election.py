import unittest

from raft.core.timers import TimeoutPolicy
from raft.sim.cluster import Cluster


class ElectionTests(unittest.TestCase):
    def test_cluster_elects_one_leader(self) -> None:
        cluster = Cluster(
            size=5,
            timeout_policy=TimeoutPolicy(150, 300),
            heartbeat_interval=30,
            network_delay_range=(5, 10),
            seed=7,
        )
        cluster.start()
        cluster.run(until=1000)

        leaders = cluster.leaders()
        self.assertEqual(len(leaders), 1)
        self.assertTrue(cluster.metrics.leader_elections)

    def test_re_election_after_leader_crash(self) -> None:
        cluster = Cluster(
            size=5,
            timeout_policy=TimeoutPolicy(150, 300),
            heartbeat_interval=30,
            network_delay_range=(5, 10),
            seed=11,
        )
        cluster.start()
        cluster.run(until=500)

        initial_leader = cluster.leaders()[0]
        cluster.crash_node(initial_leader.node_id)
        cluster.run(until=1200)

        leaders = cluster.leaders()
        self.assertEqual(len(leaders), 1)
        self.assertNotEqual(leaders[0].node_id, initial_leader.node_id)
