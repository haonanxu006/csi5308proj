from enum import Enum
from dataclasses import dataclass, field
from typing import Any


class MessageType(Enum):
    REQUEST_VOTE = 1  # request vote
    REQUEST_VOTE_RESPONSE = 2  # request vote response
    APPEND_ENTRIES = 3  # append entries (heartbeat): no actual logs here


@dataclass(frozen=True)
class Message:
    src: int
    dst: int
    term: int
    type: MessageType
    data: tuple[Any, ...] = field(default_factory=tuple)
