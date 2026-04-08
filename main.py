from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from raft.experiments.export import write_csv, write_json
from raft.experiments.runner import (
    run_delay_timeout_interaction_sweep,
    run_cluster_size_sweep,
    run_election_trials,
    run_leader_failure_sweep,
    run_leader_failure_trials,
    run_network_delay_sweep,
    run_packet_loss_sweep,
    run_timeout_range_sweep,
    summarize_election_trials,
    summarize_leader_failure_trials,
)


DEFAULT_CLUSTER_SIZE = 7
DEFAULT_CLUSTER_SIZES = "3,5,7,9,11"
DEFAULT_NETWORK_STUDY_CLUSTER_SIZE = 11
DEFAULT_TIMEOUT_RANGE = (150, 300)
DEFAULT_TIMEOUT_RANGES = "150-300,200-400,300-600,400-800"
DEFAULT_DELAY_RANGES = "1-3,5-10,20-40"
DEFAULT_DROP_RATES = "0.0,0.05,0.10,0.20"
DEFAULT_DROP_RATE_CLUSTER_SIZES = "5,7,9,11"
DEFAULT_TRIALS = 100
DEFAULT_RUNTIME = 1_500
DEFAULT_STABILIZATION_TIME = 750
DEFAULT_RECOVERY_RUNTIME = 1_500
DEFAULT_HEARTBEAT_INTERVAL = 30
DEFAULT_NETWORK_DELAY = "5-15"


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    election_parser = subparsers.add_parser("election-summary")
    _add_common_run_args(election_parser)

    recovery_parser = subparsers.add_parser("recovery-summary")
    _add_common_run_args(recovery_parser)
    recovery_parser.add_argument(
        "--stabilization-time", type=int, default=DEFAULT_STABILIZATION_TIME
    )
    recovery_parser.add_argument(
        "--recovery-runtime", type=int, default=DEFAULT_RECOVERY_RUNTIME
    )

    cluster_size_parser = subparsers.add_parser("cluster-size-sweep")
    _add_common_run_args(cluster_size_parser)
    cluster_size_parser.add_argument("--cluster-sizes", default=DEFAULT_CLUSTER_SIZES)

    timeout_parser = subparsers.add_parser("timeout-range-sweep")
    _add_common_run_args(timeout_parser)
    timeout_parser.add_argument("--timeout-ranges", default=DEFAULT_TIMEOUT_RANGES)

    delay_parser = subparsers.add_parser("network-delay-sweep")
    _add_common_run_args(
        delay_parser,
        cluster_size_default=DEFAULT_NETWORK_STUDY_CLUSTER_SIZE,
    )
    delay_parser.add_argument("--delay-ranges", default=DEFAULT_DELAY_RANGES)
    delay_parser.add_argument(
        "--stabilization-time", type=int, default=DEFAULT_STABILIZATION_TIME
    )
    delay_parser.add_argument(
        "--recovery-runtime", type=int, default=DEFAULT_RECOVERY_RUNTIME
    )

    packet_loss_parser = subparsers.add_parser("packet-loss-sweep")
    _add_common_run_args(packet_loss_parser)
    packet_loss_parser.add_argument(
        "--cluster-sizes", default=DEFAULT_DROP_RATE_CLUSTER_SIZES
    )
    packet_loss_parser.add_argument("--drop-rates", default=DEFAULT_DROP_RATES)
    packet_loss_parser.add_argument(
        "--stabilization-time", type=int, default=DEFAULT_STABILIZATION_TIME
    )
    packet_loss_parser.add_argument(
        "--recovery-runtime", type=int, default=DEFAULT_RECOVERY_RUNTIME
    )

    interaction_parser = subparsers.add_parser("delay-timeout-interaction")
    _add_common_run_args(
        interaction_parser,
        cluster_size_default=DEFAULT_NETWORK_STUDY_CLUSTER_SIZE,
    )
    interaction_parser.add_argument("--delay-ranges", default=DEFAULT_DELAY_RANGES)
    interaction_parser.add_argument("--timeout-ranges", default=DEFAULT_TIMEOUT_RANGES)
    interaction_parser.add_argument(
        "--stabilization-time", type=int, default=DEFAULT_STABILIZATION_TIME
    )
    interaction_parser.add_argument(
        "--recovery-runtime", type=int, default=DEFAULT_RECOVERY_RUNTIME
    )

    leader_failure_parser = subparsers.add_parser("leader-failure-sweep")
    _add_common_run_args(leader_failure_parser)
    leader_failure_parser.add_argument("--cluster-sizes", default=DEFAULT_CLUSTER_SIZES)
    leader_failure_parser.add_argument("--timeout-ranges", default=DEFAULT_TIMEOUT_RANGES)
    leader_failure_parser.add_argument(
        "--stabilization-time", type=int, default=DEFAULT_STABILIZATION_TIME
    )
    leader_failure_parser.add_argument(
        "--recovery-runtime", type=int, default=DEFAULT_RECOVERY_RUNTIME
    )

    full_suite_parser = subparsers.add_parser("full-suite")
    _add_common_run_args(full_suite_parser)
    full_suite_parser.add_argument("--cluster-sizes", default=DEFAULT_CLUSTER_SIZES)
    full_suite_parser.add_argument("--timeout-ranges", default=DEFAULT_TIMEOUT_RANGES)
    full_suite_parser.add_argument(
        "--stabilization-time", type=int, default=DEFAULT_STABILIZATION_TIME
    )
    full_suite_parser.add_argument(
        "--recovery-runtime", type=int, default=DEFAULT_RECOVERY_RUNTIME
    )

    args = parser.parse_args()
    payload = _run_command(args)
    print(json.dumps(payload, indent=2))


def _run_command(args: argparse.Namespace) -> dict[str, Any]:
    network_delay_range = _parse_range(args.network_delay)

    if args.command == "election-summary":
        timeout_range = (args.min_timeout, args.max_timeout)
        trials = run_election_trials(
            cluster_size=args.cluster_size,
            timeout_range=timeout_range,
            trials=args.trials,
            runtime=args.runtime,
            seed_start=args.seed_start,
            heartbeat_interval=args.heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=args.drop_rate,
        )
        summary = summarize_election_trials(trials)
        return _emit_named_outputs(
            output_dir=args.output_dir,
            summary_rows=[summary],
            trial_rows=trials,
            summary_name="election_summary",
            trials_name="election_trials",
        )

    if args.command == "recovery-summary":
        timeout_range = (args.min_timeout, args.max_timeout)
        trials = run_leader_failure_trials(
            cluster_size=args.cluster_size,
            timeout_range=timeout_range,
            trials=args.trials,
            stabilization_time=args.stabilization_time,
            recovery_runtime=args.recovery_runtime,
            seed_start=args.seed_start,
            heartbeat_interval=args.heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=args.drop_rate,
        )
        summary = summarize_leader_failure_trials(trials)
        return _emit_named_outputs(
            output_dir=args.output_dir,
            summary_rows=[summary],
            trial_rows=trials,
            summary_name="recovery_summary",
            trials_name="recovery_trials",
        )

    if args.command == "cluster-size-sweep":
        summaries, trials = run_cluster_size_sweep(
            cluster_sizes=_parse_int_list(args.cluster_sizes),
            timeout_range=(args.min_timeout, args.max_timeout),
            trials=args.trials,
            runtime=args.runtime,
            seed_start=args.seed_start,
            heartbeat_interval=args.heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=args.drop_rate,
        )
        return _emit_named_outputs(
            output_dir=args.output_dir,
            summary_rows=summaries,
            trial_rows=trials,
            summary_name="cluster_size_summaries",
            trials_name="cluster_size_trials",
        )

    if args.command == "timeout-range-sweep":
        summaries, trials = run_timeout_range_sweep(
            cluster_size=args.cluster_size,
            timeout_ranges=_parse_ranges(args.timeout_ranges),
            trials=args.trials,
            runtime=args.runtime,
            seed_start=args.seed_start,
            heartbeat_interval=args.heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=args.drop_rate,
        )
        return _emit_named_outputs(
            output_dir=args.output_dir,
            summary_rows=summaries,
            trial_rows=trials,
            summary_name="timeout_range_summaries",
            trials_name="timeout_range_trials",
        )

    if args.command == "network-delay-sweep":
        (
            election_summaries,
            election_trials,
            recovery_summaries,
            recovery_trials,
        ) = run_network_delay_sweep(
            cluster_size=args.cluster_size,
            timeout_range=(args.min_timeout, args.max_timeout),
            delay_ranges=_parse_ranges(args.delay_ranges),
            trials=args.trials,
            runtime=args.runtime,
            stabilization_time=args.stabilization_time,
            recovery_runtime=args.recovery_runtime,
            seed_start=args.seed_start,
            heartbeat_interval=args.heartbeat_interval,
            drop_rate=args.drop_rate,
        )
        return _emit_multi_outputs(
            output_dir=args.output_dir,
            datasets=[
                ("network_delay_election_summaries", election_summaries),
                ("network_delay_election_trials", election_trials),
                ("network_delay_recovery_summaries", recovery_summaries),
                ("network_delay_recovery_trials", recovery_trials),
            ],
        )

    if args.command == "packet-loss-sweep":
        (
            election_summaries,
            election_trials,
            recovery_summaries,
            recovery_trials,
        ) = run_packet_loss_sweep(
            cluster_sizes=_parse_int_list(args.cluster_sizes),
            timeout_range=(args.min_timeout, args.max_timeout),
            drop_rates=_parse_float_list(args.drop_rates),
            trials=args.trials,
            runtime=args.runtime,
            stabilization_time=args.stabilization_time,
            recovery_runtime=args.recovery_runtime,
            seed_start=args.seed_start,
            heartbeat_interval=args.heartbeat_interval,
            network_delay_range=network_delay_range,
        )
        return _emit_multi_outputs(
            output_dir=args.output_dir,
            datasets=[
                ("packet_loss_election_summaries", election_summaries),
                ("packet_loss_election_trials", election_trials),
                ("packet_loss_recovery_summaries", recovery_summaries),
                ("packet_loss_recovery_trials", recovery_trials),
            ],
        )

    if args.command == "delay-timeout-interaction":
        (
            election_summaries,
            election_trials,
            recovery_summaries,
            recovery_trials,
        ) = run_delay_timeout_interaction_sweep(
            cluster_size=args.cluster_size,
            timeout_ranges=_parse_ranges(args.timeout_ranges),
            delay_ranges=_parse_ranges(args.delay_ranges),
            trials=args.trials,
            runtime=args.runtime,
            stabilization_time=args.stabilization_time,
            recovery_runtime=args.recovery_runtime,
            seed_start=args.seed_start,
            heartbeat_interval=args.heartbeat_interval,
            drop_rate=args.drop_rate,
        )
        return _emit_multi_outputs(
            output_dir=args.output_dir,
            datasets=[
                ("delay_timeout_election_summaries", election_summaries),
                ("delay_timeout_election_trials", election_trials),
                ("delay_timeout_recovery_summaries", recovery_summaries),
                ("delay_timeout_recovery_trials", recovery_trials),
            ],
        )

    if args.command == "leader-failure-sweep":
        summaries, trials = run_leader_failure_sweep(
            cluster_sizes=_parse_int_list(args.cluster_sizes),
            timeout_ranges=_parse_ranges(args.timeout_ranges),
            trials=args.trials,
            stabilization_time=args.stabilization_time,
            recovery_runtime=args.recovery_runtime,
            seed_start=args.seed_start,
            heartbeat_interval=args.heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=args.drop_rate,
        )
        return _emit_named_outputs(
            output_dir=args.output_dir,
            summary_rows=summaries,
            trial_rows=trials,
            summary_name="leader_failure_summaries",
            trials_name="leader_failure_trials",
        )

    if args.command == "full-suite":
        cluster_sizes = _parse_int_list(args.cluster_sizes)
        timeout_ranges = _parse_ranges(args.timeout_ranges)
        election_summaries, election_trials = run_cluster_size_sweep(
            cluster_sizes=cluster_sizes,
            timeout_range=(args.min_timeout, args.max_timeout),
            trials=args.trials,
            runtime=args.runtime,
            seed_start=args.seed_start,
            heartbeat_interval=args.heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=args.drop_rate,
        )
        timeout_summaries, timeout_trials = run_timeout_range_sweep(
            cluster_size=args.cluster_size,
            timeout_ranges=timeout_ranges,
            trials=args.trials,
            runtime=args.runtime,
            seed_start=args.seed_start + (len(cluster_sizes) * args.trials),
            heartbeat_interval=args.heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=args.drop_rate,
        )
        recovery_summaries, recovery_trials = run_leader_failure_sweep(
            cluster_sizes=cluster_sizes,
            timeout_ranges=timeout_ranges,
            trials=args.trials,
            stabilization_time=args.stabilization_time,
            recovery_runtime=args.recovery_runtime,
            seed_start=args.seed_start + ((len(cluster_sizes) + len(timeout_ranges)) * args.trials),
            heartbeat_interval=args.heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=args.drop_rate,
        )
        delay_election_summaries, delay_election_trials, delay_recovery_summaries, delay_recovery_trials = run_network_delay_sweep(
            cluster_size=args.cluster_size,
            timeout_range=(args.min_timeout, args.max_timeout),
            delay_ranges=_parse_ranges(DEFAULT_DELAY_RANGES),
            trials=args.trials,
            runtime=args.runtime,
            stabilization_time=args.stabilization_time,
            recovery_runtime=args.recovery_runtime,
            seed_start=args.seed_start + (
                (len(cluster_sizes) + len(timeout_ranges) + (len(cluster_sizes) * len(timeout_ranges))) * args.trials
            ),
            heartbeat_interval=args.heartbeat_interval,
            drop_rate=args.drop_rate,
        )
        packet_loss_election_summaries, packet_loss_election_trials, packet_loss_recovery_summaries, packet_loss_recovery_trials = run_packet_loss_sweep(
            cluster_sizes=_parse_int_list(DEFAULT_DROP_RATE_CLUSTER_SIZES),
            timeout_range=(args.min_timeout, args.max_timeout),
            drop_rates=_parse_float_list(DEFAULT_DROP_RATES),
            trials=args.trials,
            runtime=args.runtime,
            stabilization_time=args.stabilization_time,
            recovery_runtime=args.recovery_runtime,
            seed_start=args.seed_start + (
                (
                    len(cluster_sizes)
                    + len(timeout_ranges)
                    + (len(cluster_sizes) * len(timeout_ranges))
                    + len(_parse_ranges(DEFAULT_DELAY_RANGES))
                    + len(_parse_ranges(DEFAULT_DELAY_RANGES))
                )
                * args.trials
            ),
            heartbeat_interval=args.heartbeat_interval,
            network_delay_range=network_delay_range,
        )
        interaction_election_summaries, interaction_election_trials, interaction_recovery_summaries, interaction_recovery_trials = run_delay_timeout_interaction_sweep(
            cluster_size=args.cluster_size,
            timeout_ranges=timeout_ranges,
            delay_ranges=_parse_ranges(DEFAULT_DELAY_RANGES),
            trials=args.trials,
            runtime=args.runtime,
            stabilization_time=args.stabilization_time,
            recovery_runtime=args.recovery_runtime,
            seed_start=args.seed_start + (
                (
                    len(cluster_sizes)
                    + len(timeout_ranges)
                    + (len(cluster_sizes) * len(timeout_ranges))
                    + (2 * len(_parse_ranges(DEFAULT_DELAY_RANGES)))
                    + (2 * len(_parse_int_list(DEFAULT_DROP_RATE_CLUSTER_SIZES)) * len(_parse_float_list(DEFAULT_DROP_RATES)))
                )
                * args.trials
            ),
            heartbeat_interval=args.heartbeat_interval,
            drop_rate=args.drop_rate,
        )
        output_dir = Path(args.output_dir) if args.output_dir else None
        written: dict[str, str] = {}
        if output_dir is not None:
            written.update(
                _write_output_set(
                    output_dir=output_dir,
                    summary_rows=election_summaries,
                    trial_rows=election_trials,
                    summary_name="cluster_size_summaries",
                    trials_name="cluster_size_trials",
                )
            )
            written.update(
                _write_output_set(
                    output_dir=output_dir,
                    summary_rows=timeout_summaries,
                    trial_rows=timeout_trials,
                    summary_name="timeout_range_summaries",
                    trials_name="timeout_range_trials",
                )
            )
            written.update(
                _write_output_set(
                    output_dir=output_dir,
                    summary_rows=recovery_summaries,
                    trial_rows=recovery_trials,
                    summary_name="leader_failure_summaries",
                    trials_name="leader_failure_trials",
                )
            )
            written.update(
                _write_output_set(
                    output_dir=output_dir,
                    summary_rows=delay_election_summaries,
                    trial_rows=delay_election_trials,
                    summary_name="network_delay_election_summaries",
                    trials_name="network_delay_election_trials",
                )
            )
            written.update(
                _write_output_set(
                    output_dir=output_dir,
                    summary_rows=delay_recovery_summaries,
                    trial_rows=delay_recovery_trials,
                    summary_name="network_delay_recovery_summaries",
                    trials_name="network_delay_recovery_trials",
                )
            )
            written.update(
                _write_output_set(
                    output_dir=output_dir,
                    summary_rows=packet_loss_election_summaries,
                    trial_rows=packet_loss_election_trials,
                    summary_name="packet_loss_election_summaries",
                    trials_name="packet_loss_election_trials",
                )
            )
            written.update(
                _write_output_set(
                    output_dir=output_dir,
                    summary_rows=packet_loss_recovery_summaries,
                    trial_rows=packet_loss_recovery_trials,
                    summary_name="packet_loss_recovery_summaries",
                    trials_name="packet_loss_recovery_trials",
                )
            )
            written.update(
                _write_output_set(
                    output_dir=output_dir,
                    summary_rows=interaction_election_summaries,
                    trial_rows=interaction_election_trials,
                    summary_name="delay_timeout_election_summaries",
                    trials_name="delay_timeout_election_trials",
                )
            )
            written.update(
                _write_output_set(
                    output_dir=output_dir,
                    summary_rows=interaction_recovery_summaries,
                    trial_rows=interaction_recovery_trials,
                    summary_name="delay_timeout_recovery_summaries",
                    trials_name="delay_timeout_recovery_trials",
                )
            )
        return {
            "cluster_size_summaries": [row.to_row() for row in election_summaries],
            "cluster_size_trials": [row.to_row() for row in election_trials],
            "timeout_range_summaries": [row.to_row() for row in timeout_summaries],
            "timeout_range_trials": [row.to_row() for row in timeout_trials],
            "leader_failure_summaries": [row.to_row() for row in recovery_summaries],
            "leader_failure_trials": [row.to_row() for row in recovery_trials],
            "network_delay_election_summaries": [row.to_row() for row in delay_election_summaries],
            "network_delay_election_trials": [row.to_row() for row in delay_election_trials],
            "network_delay_recovery_summaries": [row.to_row() for row in delay_recovery_summaries],
            "network_delay_recovery_trials": [row.to_row() for row in delay_recovery_trials],
            "packet_loss_election_summaries": [row.to_row() for row in packet_loss_election_summaries],
            "packet_loss_election_trials": [row.to_row() for row in packet_loss_election_trials],
            "packet_loss_recovery_summaries": [row.to_row() for row in packet_loss_recovery_summaries],
            "packet_loss_recovery_trials": [row.to_row() for row in packet_loss_recovery_trials],
            "delay_timeout_election_summaries": [row.to_row() for row in interaction_election_summaries],
            "delay_timeout_election_trials": [row.to_row() for row in interaction_election_trials],
            "delay_timeout_recovery_summaries": [row.to_row() for row in interaction_recovery_summaries],
            "delay_timeout_recovery_trials": [row.to_row() for row in interaction_recovery_trials],
            "written_files": written,
        }

    raise ValueError(f"unsupported command: {args.command}")


def _emit_named_outputs(
    output_dir: str | None,
    summary_rows: list[Any],
    trial_rows: list[Any],
    summary_name: str,
    trials_name: str,
) -> dict[str, Any]:
    written = {}
    if output_dir:
        written = _write_output_set(
            output_dir=Path(output_dir),
            summary_rows=summary_rows,
            trial_rows=trial_rows,
            summary_name=summary_name,
            trials_name=trials_name,
        )
    return {
        "summaries": [row.to_row() for row in summary_rows],
        "trials": [row.to_row() for row in trial_rows],
        "written_files": written,
    }


def _emit_multi_outputs(
    output_dir: str | None,
    datasets: list[tuple[str, list[Any]]],
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    written: dict[str, str] = {}
    output_path = Path(output_dir) if output_dir else None
    for dataset_name, rows in datasets:
        payload[dataset_name] = [row.to_row() for row in rows]
        if output_path is not None:
            csv_path = write_csv(output_path / f"{dataset_name}.csv", rows)
            json_path = write_json(output_path / f"{dataset_name}.json", rows)
            written[f"{dataset_name}_csv"] = str(csv_path)
            written[f"{dataset_name}_json"] = str(json_path)
    payload["written_files"] = written
    return payload


def _write_output_set(
    output_dir: Path,
    summary_rows: list[Any],
    trial_rows: list[Any],
    summary_name: str,
    trials_name: str,
) -> dict[str, str]:
    summary_csv = write_csv(output_dir / f"{summary_name}.csv", summary_rows)
    trials_csv = write_csv(output_dir / f"{trials_name}.csv", trial_rows)
    summary_json = write_json(output_dir / f"{summary_name}.json", summary_rows)
    trials_json = write_json(output_dir / f"{trials_name}.json", trial_rows)
    return {
        f"{summary_name}_csv": str(summary_csv),
        f"{trials_name}_csv": str(trials_csv),
        f"{summary_name}_json": str(summary_json),
        f"{trials_name}_json": str(trials_json),
    }


def _add_common_run_args(
    parser: argparse.ArgumentParser,
    cluster_size_default: int = DEFAULT_CLUSTER_SIZE,
) -> None:
    parser.add_argument("--cluster-size", type=int, default=cluster_size_default)
    parser.add_argument(
        "--min-timeout", type=int, default=DEFAULT_TIMEOUT_RANGE[0]
    )
    parser.add_argument(
        "--max-timeout", type=int, default=DEFAULT_TIMEOUT_RANGE[1]
    )
    parser.add_argument("--trials", type=int, default=DEFAULT_TRIALS)
    parser.add_argument("--runtime", type=int, default=DEFAULT_RUNTIME)
    parser.add_argument("--seed-start", type=int, default=0)
    parser.add_argument(
        "--heartbeat-interval", type=int, default=DEFAULT_HEARTBEAT_INTERVAL
    )
    parser.add_argument("--network-delay", default=DEFAULT_NETWORK_DELAY)
    parser.add_argument("--drop-rate", type=float, default=0.0)
    parser.add_argument("--output-dir")


def _parse_int_list(raw_value: str) -> list[int]:
    return [int(part.strip()) for part in raw_value.split(",") if part.strip()]


def _parse_ranges(raw_value: str) -> list[tuple[int, int]]:
    return [_parse_range(part.strip()) for part in raw_value.split(",") if part.strip()]


def _parse_float_list(raw_value: str) -> list[float]:
    return [float(part.strip()) for part in raw_value.split(",") if part.strip()]


def _parse_range(raw_value: str) -> tuple[int, int]:
    left, right = raw_value.split("-", maxsplit=1)
    return int(left), int(right)


if __name__ == "__main__":
    main()
