import tempfile
import unittest
from pathlib import Path

from raft.experiments.export import write_csv
from raft.experiments.runner import (
    run_delay_timeout_interaction_sweep,
    run_cluster_size_sweep,
    run_leader_failure_sweep,
    run_network_delay_sweep,
    run_packet_loss_sweep,
    run_single_election_trial,
    run_timeout_range_sweep,
)


class ExperimentTests(unittest.TestCase):
    def test_basic_election_runner_output(self) -> None:
        result = run_single_election_trial(
            cluster_size=5,
            timeout_range=(150, 300),
            runtime=1_000,
            seed=7,
        )

        self.assertEqual(result.seed, 7)
        self.assertEqual(result.cluster_size, 5)
        self.assertEqual(result.timeout_min, 150)
        self.assertEqual(result.timeout_max, 300)
        self.assertIsNotNone(result.elected_leader_id)
        self.assertIsNotNone(result.election_time)
        self.assertIsNotNone(result.elected_term)
        self.assertGreater(result.total_messages, 0)
        self.assertGreaterEqual(result.dropped_messages, 0)

    def test_cluster_size_sweep_returns_expected_number_of_rows(self) -> None:
        summaries, trials = run_cluster_size_sweep(
            cluster_sizes=[3, 5, 7],
            timeout_range=(150, 300),
            trials=2,
            runtime=1_000,
        )

        self.assertEqual(len(summaries), 3)
        self.assertEqual(len(trials), 6)

    def test_timeout_sweep_returns_expected_number_of_rows(self) -> None:
        summaries, trials = run_timeout_range_sweep(
            cluster_size=5,
            timeout_ranges=[(150, 300), (200, 400)],
            trials=3,
            runtime=1_000,
        )

        self.assertEqual(len(summaries), 2)
        self.assertEqual(len(trials), 6)

    def test_leader_failure_sweep_returns_expected_number_of_rows(self) -> None:
        summaries, trials = run_leader_failure_sweep(
            cluster_sizes=[3, 5],
            timeout_ranges=[(150, 300), (200, 400)],
            trials=2,
            stabilization_time=500,
            recovery_runtime=700,
        )

        self.assertEqual(len(summaries), 4)
        self.assertEqual(len(trials), 8)

    def test_network_delay_sweep_returns_expected_number_of_rows(self) -> None:
        election_summaries, election_trials, recovery_summaries, recovery_trials = (
            run_network_delay_sweep(
                cluster_size=5,
                timeout_range=(150, 300),
                delay_ranges=[(1, 3), (5, 10), (20, 40)],
                trials=2,
                runtime=1_000,
                stabilization_time=500,
                recovery_runtime=700,
            )
        )

        self.assertEqual(len(election_summaries), 3)
        self.assertEqual(len(election_trials), 6)
        self.assertEqual(len(recovery_summaries), 3)
        self.assertEqual(len(recovery_trials), 6)

    def test_packet_loss_sweep_returns_expected_number_of_rows(self) -> None:
        election_summaries, election_trials, recovery_summaries, recovery_trials = (
            run_packet_loss_sweep(
                cluster_sizes=[3, 5],
                timeout_range=(150, 300),
                drop_rates=[0.0, 0.1, 0.2],
                trials=2,
                runtime=1_000,
                stabilization_time=500,
                recovery_runtime=700,
            )
        )

        self.assertEqual(len(election_summaries), 6)
        self.assertEqual(len(election_trials), 12)
        self.assertEqual(len(recovery_summaries), 6)
        self.assertEqual(len(recovery_trials), 12)

    def test_delay_timeout_interaction_returns_expected_number_of_rows(self) -> None:
        election_summaries, election_trials, recovery_summaries, recovery_trials = (
            run_delay_timeout_interaction_sweep(
                cluster_size=5,
                timeout_ranges=[(150, 300), (200, 400)],
                delay_ranges=[(1, 3), (5, 10), (20, 40)],
                trials=2,
                runtime=1_000,
                stabilization_time=500,
                recovery_runtime=700,
            )
        )

        self.assertEqual(len(election_summaries), 6)
        self.assertEqual(len(election_trials), 12)
        self.assertEqual(len(recovery_summaries), 6)
        self.assertEqual(len(recovery_trials), 12)

    def test_csv_export_writes_files(self) -> None:
        summaries, _ = run_cluster_size_sweep(
            cluster_sizes=[3],
            timeout_range=(150, 300),
            trials=1,
            runtime=1_000,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_csv(Path(tmpdir) / "summaries.csv", summaries)
            self.assertTrue(path.exists())
            contents = path.read_text(encoding="utf-8")
            self.assertIn("cluster_size", contents)
            self.assertIn("successful_trials", contents)
