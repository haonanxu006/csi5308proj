from enum import Enum

class NodeRole(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3