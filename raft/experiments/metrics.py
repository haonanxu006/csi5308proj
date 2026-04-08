from dataclasses import dataclass, field


@dataclass()
class ElectionRecord:
    term: int
    leader_id: int
    elected_at: int
    election_duration: int


@dataclass()
class Metrics:
    election_starts: list[tuple[int, int]] = field(default_factory=list)
    leader_elections: list[ElectionRecord] = field(default_factory=list)

    def record_election_started(self, term: int, started_at: int) -> None:
        self.election_starts.append((term, started_at))

    def first_election_start_for_term(self, term: int) -> int | None:
        starts = [started_at for seen_term, started_at in self.election_starts if seen_term == term]
        if not starts:
            return None
        return min(starts)

    def record_leader_elected(
        self, term: int, leader_id: int, elected_at: int, election_duration: int
    ) -> None:
        self.leader_elections.append(
            ElectionRecord(term, leader_id, elected_at, election_duration)
        )
