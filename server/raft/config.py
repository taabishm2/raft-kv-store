import enum
from os import environ, getenv
from random import randrange

from .utils import *


class Globals():
    def __init__(self):
        # Persistent state on all servers. Updated on stable storage before responding to RPCs
        # TODO: Make these persistent
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
        # TODO: Remove this after election is setup.
        self.state = NodeRole.Follower 
        if environ['IS_LEADER'] == "TRUE":
           self.state = NodeRole.Leader

        self.is_unresponsive = False
        if getenv('IS_UNRESPONSIVE', False) == 'TRUE':
            self.is_unresponsive = True
        log_me(f"is_unresponsive? {self.is_unresponsive}")

        # Other state
        self.leader_ip = None

        self.election_timeout = 150
        self.curr_rand_election_timeout = 0

        # Syntax: os.getenv(key, default).
        # Heartbeat timeout T= 250ms. Random timeout in range [T, 2T] unless specified in the env vars
        self.LOW_TIMEOUT = int(getenv('LOW_TIMEOUT', 250))
        self.HIGH_TIMEOUT = int(getenv('HIGH_TIMEOUT', 500))

        # REQUESTS_TIMEOUT = 50
        # Heartbeat is sent every 100ms
        self.HB_TIME = int(getenv('HB_TIME', 100))

        log_me("Global config initialized")

        # MAX_LOG_WAIT = int(getenv('MAX_LOG_WAIT', 150))

        # TODO Move out of here
        def chunks(l, n):
            n = max(1, n)
            return (l[i:i + n] for i in range(0, len(l), n))


class NodeRole(enum.Enum):
    Follower = 1
    Candidate = 2
    Leader = 3


globals = Globals()
