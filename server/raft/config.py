from os import getenv, environ
from random import randrange
import enum


class Globals():
    def __init__(self):
        # Persistent state on all servers. Updated on stable storage before responding to RPCs
        self.current_term = 0  # latest term server has seen
        self.voted_for = None  # candidateId that received vote in current term

        # Volatile state on all servers
        self.commitIndex = 0  # index of highest log entry known to be committed
        self.lastApplied = 0  # index of highest log entry applied to state machine

        # Volatile state on leaders. Reinitialized after election
        self.nextIndex = []  # for each server, index of the next log entry to send to that server
        self.matchIndex = []  # for each server, index of highest log entry known to be replicated on server

        # Raft-node state
        self.name = environ['NAME']
        self.state = NodeRole.Leader if environ['IS_LEADER'] else NodeRole.Follower

        # Other state
        self.leader_ip = None

        # Syntax: os.getenv(key, default).
        # Heartbeat timeout T= 250ms. Random timeout in range [T, 2T] unless specified in the env vars
        self.LOW_TIMEOUT = int(getenv('LOW_TIMEOUT', 250))
        self.HIGH_TIMEOUT = int(getenv('HIGH_TIMEOUT', 500))

        # REQUESTS_TIMEOUT = 50
        # Heartbeat is sent every 100ms
        self.HB_TIME = int(getenv('HB_TIME', 100))

        # MAX_LOG_WAIT = int(getenv('MAX_LOG_WAIT', 150))

        # TODO Move out of here
        def random_timeout():
            return randrange(self.LOW_TIMEOUT, self.HIGH_TIMEOUT) / 1000

        # TODO Move out of here
        def chunks(l, n):
            n = max(1, n)
            return (l[i:i + n] for i in range(0, len(l), n))


class NodeRole(enum.Enum):
    Follower = 1
    Candidate = 2
    Leader = 3


globals = Globals()
