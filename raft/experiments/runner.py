from __future__ import annotations

from dataclasses import asdict, dataclass
from statistics import mean, stdev

from raft.core.timers import TimeoutPolicy
from raft.sim.cluster import Cluster


DEFAULT_HEARTBEAT_INTERVAL = 30
DEFAULT_NETWORK_DELAY_RANGE = (5, 10)


@dataclass(slots=True)
class ElectionTrialResult:
    seed: int
    cluster_size: int
    timeout_min: int
    timeout_max: int
    runtime: int
    heartbeat_interval: int
    network_delay_min: int
    network_delay_max: int
    drop_rate: float
    elected_leader_id: int | None
    election_time: int | None
    elected_term: int | None
    total_messages: int
    dropped_messages: int

    def to_row(self) -> dict[str, int | float | str | None]:
        return asdict(self)


@dataclass(slots=True)
class LeaderFailureTrialResult:
    seed: int
    cluster_size: int
    timeout_min: int
    timeout_max: int
    stabilization_time: int
    recovery_runtime: int
    heartbeat_interval: int
    network_delay_min: int
    network_delay_max: int
    drop_rate: float
    initial_leader_id: int | None
    new_leader_id: int | None
    reelection_time: int | None
    reelected_term: int | None
    terms_observed: tuple[int, ...]
    total_messages: int
    dropped_messages: int

    def to_row(self) -> dict[str, int | float | str | None]:
        row = asdict(self)
        row["terms_observed"] = ",".join(str(term) for term in self.terms_observed)
        row["terms_observed_count"] = len(self.terms_observed)
        return row


@dataclass(slots=True)
class ElectionSummary:
    cluster_size: int
    timeout_min: int
    timeout_max: int
    heartbeat_interval: int
    network_delay_min: int
    network_delay_max: int
    drop_rate: float
    trials: int
    successful_trials: int
    incomplete_election_rate: float
    avg_election_time: float | None
    stdev_election_time: float | None
    avg_messages: float
    stdev_messages: float
    avg_dropped_messages: float
    stdev_dropped_messages: float

    def to_row(self) -> dict[str, int | float | None]:
        return asdict(self)


@dataclass(slots=True)
class LeaderFailureSummary:
    cluster_size: int
    timeout_min: int
    timeout_max: int
    heartbeat_interval: int
    network_delay_min: int
    network_delay_max: int
    drop_rate: float
    trials: int
    successful_trials: int
    incomplete_reelection_rate: float
    avg_reelection_time: float | None
    stdev_reelection_time: float | None
    avg_messages: float
    stdev_messages: float
    avg_dropped_messages: float
    stdev_dropped_messages: float
    avg_terms_observed: float

    def to_row(self) -> dict[str, int | float | None]:
        return asdict(self)


def run_single_election_trial(
    cluster_size: int,
    timeout_range: tuple[int, int],
    runtime: int = 1_000,
    seed: int = 0,
    heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
    network_delay_range: tuple[int, int] = DEFAULT_NETWORK_DELAY_RANGE,
    drop_rate: float = 0.0,
) -> ElectionTrialResult:
    cluster = _build_cluster(
        cluster_size=cluster_size,
        timeout_range=timeout_range,
        seed=seed,
        heartbeat_interval=heartbeat_interval,
        network_delay_range=network_delay_range,
        drop_rate=drop_rate,
    )
    cluster.start()
    cluster.run(until=runtime)
    election = cluster.metrics.leader_elections[0] if cluster.metrics.leader_elections else None
    return ElectionTrialResult(
        seed=seed,
        cluster_size=cluster_size,
        timeout_min=timeout_range[0],
        timeout_max=timeout_range[1],
        runtime=runtime,
        heartbeat_interval=heartbeat_interval,
        network_delay_min=network_delay_range[0],
        network_delay_max=network_delay_range[1],
        drop_rate=drop_rate,
        elected_leader_id=None if election is None else election.leader_id,
        election_time=None if election is None else election.election_duration,
        elected_term=None if election is None else election.term,
        total_messages=cluster.network.message_count,
        dropped_messages=cluster.network.dropped_count,
    )


def run_leader_failure_trial(
    cluster_size: int,
    timeout_range: tuple[int, int],
    stabilization_time: int = 500,
    recovery_runtime: int = 700,
    seed: int = 0,
    heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
    network_delay_range: tuple[int, int] = DEFAULT_NETWORK_DELAY_RANGE,
    drop_rate: float = 0.0,
) -> LeaderFailureTrialResult:
    cluster = _build_cluster(
        cluster_size=cluster_size,
        timeout_range=timeout_range,
        seed=seed,
        heartbeat_interval=heartbeat_interval,
        network_delay_range=network_delay_range,
        drop_rate=drop_rate,
    )
    cluster.start()
    cluster.run(until=stabilization_time)

    leaders = cluster.leaders()
    if not leaders:
        return LeaderFailureTrialResult(
            seed=seed,
            cluster_size=cluster_size,
            timeout_min=timeout_range[0],
            timeout_max=timeout_range[1],
            stabilization_time=stabilization_time,
            recovery_runtime=recovery_runtime,
            heartbeat_interval=heartbeat_interval,
            network_delay_min=network_delay_range[0],
            network_delay_max=network_delay_range[1],
            drop_rate=drop_rate,
            initial_leader_id=None,
            new_leader_id=None,
            reelection_time=None,
            reelected_term=None,
            terms_observed=_terms_observed(cluster),
            total_messages=cluster.network.message_count,
            dropped_messages=cluster.network.dropped_count,
        )

    initial_leader = leaders[0]
    elections_before_crash = len(cluster.metrics.leader_elections)
    crash_time = cluster.event_loop.time
    cluster.crash_node(initial_leader.node_id)
    cluster.run(until=stabilization_time + recovery_runtime)

    later_elections = cluster.metrics.leader_elections[elections_before_crash:]
    reelection = next(
        (record for record in later_elections if record.leader_id != initial_leader.node_id),
        None,
    )
    return LeaderFailureTrialResult(
        seed=seed,
        cluster_size=cluster_size,
        timeout_min=timeout_range[0],
        timeout_max=timeout_range[1],
        stabilization_time=stabilization_time,
        recovery_runtime=recovery_runtime,
        heartbeat_interval=heartbeat_interval,
        network_delay_min=network_delay_range[0],
        network_delay_max=network_delay_range[1],
        drop_rate=drop_rate,
        initial_leader_id=initial_leader.node_id,
        new_leader_id=None if reelection is None else reelection.leader_id,
        reelection_time=None if reelection is None else reelection.elected_at - crash_time,
        reelected_term=None if reelection is None else reelection.term,
        terms_observed=_terms_observed(cluster),
        total_messages=cluster.network.message_count,
        dropped_messages=cluster.network.dropped_count,
    )


def run_election_trials(
    cluster_size: int,
    timeout_range: tuple[int, int],
    trials: int,
    runtime: int = 1_000,
    seed_start: int = 0,
    heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
    network_delay_range: tuple[int, int] = DEFAULT_NETWORK_DELAY_RANGE,
    drop_rate: float = 0.0,
) -> list[ElectionTrialResult]:
    return [
        run_single_election_trial(
            cluster_size=cluster_size,
            timeout_range=timeout_range,
            runtime=runtime,
            seed=seed_start + offset,
            heartbeat_interval=heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=drop_rate,
        )
        for offset in range(trials)
    ]


def run_leader_failure_trials(
    cluster_size: int,
    timeout_range: tuple[int, int],
    trials: int,
    stabilization_time: int = 500,
    recovery_runtime: int = 700,
    seed_start: int = 0,
    heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
    network_delay_range: tuple[int, int] = DEFAULT_NETWORK_DELAY_RANGE,
    drop_rate: float = 0.0,
) -> list[LeaderFailureTrialResult]:
    return [
        run_leader_failure_trial(
            cluster_size=cluster_size,
            timeout_range=timeout_range,
            stabilization_time=stabilization_time,
            recovery_runtime=recovery_runtime,
            seed=seed_start + offset,
            heartbeat_interval=heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=drop_rate,
        )
        for offset in range(trials)
    ]


def summarize_election_trials(results: list[ElectionTrialResult]) -> ElectionSummary:
    if not results:
        raise ValueError("expected at least one election trial result")
    successful = [result for result in results if result.election_time is not None]
    trials = len(results)
    incomplete_rate = 1.0 - (len(successful) / trials)
    election_times = [result.election_time for result in successful]
    messages = [result.total_messages for result in results]
    dropped = [result.dropped_messages for result in results]
    return ElectionSummary(
        cluster_size=results[0].cluster_size,
        timeout_min=results[0].timeout_min,
        timeout_max=results[0].timeout_max,
        heartbeat_interval=results[0].heartbeat_interval,
        network_delay_min=results[0].network_delay_min,
        network_delay_max=results[0].network_delay_max,
        drop_rate=results[0].drop_rate,
        trials=trials,
        successful_trials=len(successful),
        incomplete_election_rate=incomplete_rate,
        avg_election_time=None if not election_times else mean(election_times),
        stdev_election_time=None if len(election_times) < 2 else stdev(election_times),
        avg_messages=mean(messages),
        stdev_messages=stdev(messages) if len(messages) >= 2 else 0.0,
        avg_dropped_messages=mean(dropped),
        stdev_dropped_messages=stdev(dropped) if len(dropped) >= 2 else 0.0,
    )


def summarize_leader_failure_trials(
    results: list[LeaderFailureTrialResult],
) -> LeaderFailureSummary:
    if not results:
        raise ValueError("expected at least one leader failure trial result")
    successful = [result for result in results if result.reelection_time is not None]
    trials = len(results)
    reelection_times = [result.reelection_time for result in successful]
    messages = [result.total_messages for result in results]
    dropped = [result.dropped_messages for result in results]
    return LeaderFailureSummary(
        cluster_size=results[0].cluster_size,
        timeout_min=results[0].timeout_min,
        timeout_max=results[0].timeout_max,
        heartbeat_interval=results[0].heartbeat_interval,
        network_delay_min=results[0].network_delay_min,
        network_delay_max=results[0].network_delay_max,
        drop_rate=results[0].drop_rate,
        trials=trials,
        successful_trials=len(successful),
        incomplete_reelection_rate=1.0 - (len(successful) / trials),
        avg_reelection_time=None if not reelection_times else mean(reelection_times),
        stdev_reelection_time=None if len(reelection_times) < 2 else stdev(reelection_times),
        avg_messages=mean(messages),
        stdev_messages=stdev(messages) if len(messages) >= 2 else 0.0,
        avg_dropped_messages=mean(dropped),
        stdev_dropped_messages=stdev(dropped) if len(dropped) >= 2 else 0.0,
        avg_terms_observed=mean(len(result.terms_observed) for result in results),
    )


def run_cluster_size_sweep(
    cluster_sizes: list[int],
    timeout_range: tuple[int, int],
    trials: int,
    runtime: int = 1_000,
    seed_start: int = 0,
    heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
    network_delay_range: tuple[int, int] = DEFAULT_NETWORK_DELAY_RANGE,
    drop_rate: float = 0.0,
) -> tuple[list[ElectionSummary], list[ElectionTrialResult]]:
    summaries: list[ElectionSummary] = []
    trial_rows: list[ElectionTrialResult] = []
    next_seed = seed_start
    for cluster_size in cluster_sizes:
        results = run_election_trials(
            cluster_size=cluster_size,
            timeout_range=timeout_range,
            trials=trials,
            runtime=runtime,
            seed_start=next_seed,
            heartbeat_interval=heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=drop_rate,
        )
        summaries.append(summarize_election_trials(results))
        trial_rows.extend(results)
        next_seed += trials
    return summaries, trial_rows


def run_timeout_range_sweep(
    cluster_size: int,
    timeout_ranges: list[tuple[int, int]],
    trials: int,
    runtime: int = 1_000,
    seed_start: int = 0,
    heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
    network_delay_range: tuple[int, int] = DEFAULT_NETWORK_DELAY_RANGE,
    drop_rate: float = 0.0,
) -> tuple[list[ElectionSummary], list[ElectionTrialResult]]:
    summaries: list[ElectionSummary] = []
    trial_rows: list[ElectionTrialResult] = []
    next_seed = seed_start
    for timeout_range in timeout_ranges:
        results = run_election_trials(
            cluster_size=cluster_size,
            timeout_range=timeout_range,
            trials=trials,
            runtime=runtime,
            seed_start=next_seed,
            heartbeat_interval=heartbeat_interval,
            network_delay_range=network_delay_range,
            drop_rate=drop_rate,
        )
        summaries.append(summarize_election_trials(results))
        trial_rows.extend(results)
        next_seed += trials
    return summaries, trial_rows


def run_leader_failure_sweep(
    cluster_sizes: list[int],
    timeout_ranges: list[tuple[int, int]],
    trials: int,
    stabilization_time: int = 500,
    recovery_runtime: int = 700,
    seed_start: int = 0,
    heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
    network_delay_range: tuple[int, int] = DEFAULT_NETWORK_DELAY_RANGE,
    drop_rate: float = 0.0,
) -> tuple[list[LeaderFailureSummary], list[LeaderFailureTrialResult]]:
    summaries: list[LeaderFailureSummary] = []
    trial_rows: list[LeaderFailureTrialResult] = []
    next_seed = seed_start
    for cluster_size in cluster_sizes:
        for timeout_range in timeout_ranges:
            results = run_leader_failure_trials(
                cluster_size=cluster_size,
                timeout_range=timeout_range,
                trials=trials,
                stabilization_time=stabilization_time,
                recovery_runtime=recovery_runtime,
                seed_start=next_seed,
                heartbeat_interval=heartbeat_interval,
                network_delay_range=network_delay_range,
                drop_rate=drop_rate,
            )
            summaries.append(summarize_leader_failure_trials(results))
            trial_rows.extend(results)
            next_seed += trials
    return summaries, trial_rows


def run_network_delay_sweep(
    cluster_size: int,
    timeout_range: tuple[int, int],
    delay_ranges: list[tuple[int, int]],
    trials: int,
    runtime: int = 1_000,
    stabilization_time: int = 500,
    recovery_runtime: int = 700,
    seed_start: int = 0,
    heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
    drop_rate: float = 0.0,
) -> tuple[
    list[ElectionSummary],
    list[ElectionTrialResult],
    list[LeaderFailureSummary],
    list[LeaderFailureTrialResult],
]:
    election_summaries: list[ElectionSummary] = []
    election_trials: list[ElectionTrialResult] = []
    recovery_summaries: list[LeaderFailureSummary] = []
    recovery_trials: list[LeaderFailureTrialResult] = []
    next_seed = seed_start
    for delay_range in delay_ranges:
        election_results = run_election_trials(
            cluster_size=cluster_size,
            timeout_range=timeout_range,
            trials=trials,
            runtime=runtime,
            seed_start=next_seed,
            heartbeat_interval=heartbeat_interval,
            network_delay_range=delay_range,
            drop_rate=drop_rate,
        )
        election_summaries.append(summarize_election_trials(election_results))
        election_trials.extend(election_results)
        next_seed += trials

        recovery_results = run_leader_failure_trials(
            cluster_size=cluster_size,
            timeout_range=timeout_range,
            trials=trials,
            stabilization_time=stabilization_time,
            recovery_runtime=recovery_runtime,
            seed_start=next_seed,
            heartbeat_interval=heartbeat_interval,
            network_delay_range=delay_range,
            drop_rate=drop_rate,
        )
        recovery_summaries.append(summarize_leader_failure_trials(recovery_results))
        recovery_trials.extend(recovery_results)
        next_seed += trials
    return (
        election_summaries,
        election_trials,
        recovery_summaries,
        recovery_trials,
    )


def run_packet_loss_sweep(
    cluster_sizes: list[int],
    timeout_range: tuple[int, int],
    drop_rates: list[float],
    trials: int,
    runtime: int = 1_000,
    stabilization_time: int = 500,
    recovery_runtime: int = 700,
    seed_start: int = 0,
    heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
    network_delay_range: tuple[int, int] = DEFAULT_NETWORK_DELAY_RANGE,
) -> tuple[
    list[ElectionSummary],
    list[ElectionTrialResult],
    list[LeaderFailureSummary],
    list[LeaderFailureTrialResult],
]:
    election_summaries: list[ElectionSummary] = []
    election_trials: list[ElectionTrialResult] = []
    recovery_summaries: list[LeaderFailureSummary] = []
    recovery_trials: list[LeaderFailureTrialResult] = []
    next_seed = seed_start
    for cluster_size in cluster_sizes:
        for drop_rate in drop_rates:
            election_results = run_election_trials(
                cluster_size=cluster_size,
                timeout_range=timeout_range,
                trials=trials,
                runtime=runtime,
                seed_start=next_seed,
                heartbeat_interval=heartbeat_interval,
                network_delay_range=network_delay_range,
                drop_rate=drop_rate,
            )
            election_summaries.append(summarize_election_trials(election_results))
            election_trials.extend(election_results)
            next_seed += trials

            recovery_results = run_leader_failure_trials(
                cluster_size=cluster_size,
                timeout_range=timeout_range,
                trials=trials,
                stabilization_time=stabilization_time,
                recovery_runtime=recovery_runtime,
                seed_start=next_seed,
                heartbeat_interval=heartbeat_interval,
                network_delay_range=network_delay_range,
                drop_rate=drop_rate,
            )
            recovery_summaries.append(summarize_leader_failure_trials(recovery_results))
            recovery_trials.extend(recovery_results)
            next_seed += trials
    return (
        election_summaries,
        election_trials,
        recovery_summaries,
        recovery_trials,
    )


def run_delay_timeout_interaction_sweep(
    cluster_size: int,
    timeout_ranges: list[tuple[int, int]],
    delay_ranges: list[tuple[int, int]],
    trials: int,
    runtime: int = 1_000,
    stabilization_time: int = 500,
    recovery_runtime: int = 700,
    seed_start: int = 0,
    heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
    drop_rate: float = 0.0,
) -> tuple[
    list[ElectionSummary],
    list[ElectionTrialResult],
    list[LeaderFailureSummary],
    list[LeaderFailureTrialResult],
]:
    election_summaries: list[ElectionSummary] = []
    election_trials: list[ElectionTrialResult] = []
    recovery_summaries: list[LeaderFailureSummary] = []
    recovery_trials: list[LeaderFailureTrialResult] = []
    next_seed = seed_start
    for timeout_range in timeout_ranges:
        for delay_range in delay_ranges:
            election_results = run_election_trials(
                cluster_size=cluster_size,
                timeout_range=timeout_range,
                trials=trials,
                runtime=runtime,
                seed_start=next_seed,
                heartbeat_interval=heartbeat_interval,
                network_delay_range=delay_range,
                drop_rate=drop_rate,
            )
            election_summaries.append(summarize_election_trials(election_results))
            election_trials.extend(election_results)
            next_seed += trials

            recovery_results = run_leader_failure_trials(
                cluster_size=cluster_size,
                timeout_range=timeout_range,
                trials=trials,
                stabilization_time=stabilization_time,
                recovery_runtime=recovery_runtime,
                seed_start=next_seed,
                heartbeat_interval=heartbeat_interval,
                network_delay_range=delay_range,
                drop_rate=drop_rate,
            )
            recovery_summaries.append(summarize_leader_failure_trials(recovery_results))
            recovery_trials.extend(recovery_results)
            next_seed += trials
    return (
        election_summaries,
        election_trials,
        recovery_summaries,
        recovery_trials,
    )


def run_batch(
    cluster_size: int,
    timeout_range: tuple[int, int],
    trials: int,
    runtime: int = 1_000,
) -> dict[str, int | float | None]:
    return summarize_election_trials(
        run_election_trials(
            cluster_size=cluster_size,
            timeout_range=timeout_range,
            trials=trials,
            runtime=runtime,
        )
    ).to_row()


def run_recovery_batch(
    cluster_size: int,
    timeout_range: tuple[int, int],
    trials: int,
    stabilization_time: int = 500,
    runtime: int = 1_200,
) -> dict[str, int | float | None]:
    return summarize_leader_failure_trials(
        run_leader_failure_trials(
            cluster_size=cluster_size,
            timeout_range=timeout_range,
            trials=trials,
            stabilization_time=stabilization_time,
            recovery_runtime=max(0, runtime - stabilization_time),
        )
    ).to_row()


def _build_cluster(
    cluster_size: int,
    timeout_range: tuple[int, int],
    seed: int,
    heartbeat_interval: int,
    network_delay_range: tuple[int, int],
    drop_rate: float,
) -> Cluster:
    return Cluster(
        size=cluster_size,
        timeout_policy=TimeoutPolicy(*timeout_range),
        heartbeat_interval=heartbeat_interval,
        network_delay_range=network_delay_range,
        drop_rate=drop_rate,
        seed=seed,
    )


def _terms_observed(cluster: Cluster) -> tuple[int, ...]:
    return tuple(sorted({node.current_term for node in cluster.nodes}))
