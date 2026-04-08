from dataclasses import dataclass
import random
from random import Random


@dataclass(frozen=True)
class TimeoutPolicy:
    """
    Raft timeout policy: random timeout between a range
    """

    min_election_timeout: int
    max_election_timeout: int
    rng: Random | None = None

    def sample(self) -> int:
        if self.rng is None:
            return random.randint(
                self.min_election_timeout,
                self.max_election_timeout,
            )
        return self.rng.randint(
            self.min_election_timeout,
            self.max_election_timeout,
        )
