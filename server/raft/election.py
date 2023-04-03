import time
from threading import Thread

from .config import NodeRole, globals
from .utils import *
from .transport import transport
from .log_manager import log_manager


class Election:

    def __init__(self):
        # Start a daemon thread to watch for callbacks from leader.
        self.timeout_thread = None
        # self.init_heartbeat()

        # TODO: Need to add a function that triggers the FIRST election.
        # Initially all nodes will start as followers. Wait for one timeout and start election i guess?

    # TODO:Who triggers this? This should be running in the bg continuously for all servers
    # Reply(Sweksha): This will only be run when voted as leader.
    def init_heartbeat(self):
        """Initiate periodic heartbeats to the follower nodes if node is leader"""
        if globals.state != NodeRole.Leader: return

        log_me(f"Node is leader for the term {globals.current_term}, Starting periodic heartbeats to peers")
        # send heartbeat to follower peers
        for peer in transport.peer_ips:
            Thread(target=self.send_heartbeat, args=(peer,)).start()

    def send_heartbeat(self, peer: str):
        """SEND heartbeat to the peers and get response if LEADER"""

        try:
            while globals.state == NodeRole.Leader:
                log_me(f'[PEER HEARTBEAT] {peer}')
                start = time.time()
                response = transport.send_heartbeat(peer=peer)
                if response:
                    # Peer has higher term. Relinquish leadership
                    if response.term > globals.current_term:
                        globals.current_term = response.term
                        globals.state = NodeRole.Follower
                        self.init_timeout()
                delta = time.time() - start
                time.sleep((globals.HB_TIME - delta) / 1000)
                print(f'[PEER HEARTBEAT RESPONSE] {peer} {response}')
        except Exception as e:
            raise e

    def timeout_loop(self):
        '''
        if this node is not the leader, wait for the leader
        to send the heartbeat. If heartbeat is not received by the follower,
        within some unit time then start the election. This loop will
        run endlessly
        '''
        while globals.state != NodeRole.Leader:
            delta = globals.curr_rand_election_timeout - time.time()
            if delta < 0:
                self.trigger_election()
                print("TODO")
            else:
                time.sleep(delta)

    def init_timeout(self):
        """Checks for missed heartbeats from the leader and start the election"""

        try:
            log_me('Starting timeout')
            rand_timeout = random_timeout(globals.LOW_TIMEOUT, globals.HIGH_TIMEOUT)
            globals.curr_rand_election_timeout = time.time() + rand_timeout
            if self.timeout_thread and self.timeout_thread.is_alive():
                return
            self.timeout_thread = Thread(target=self.timeout_loop)
            self.timeout_thread.start()
        except Exception as e:
            raise e

    def trigger_election(self):
        log_me(f"{globals.name} triggered an election!")
        globals.current_term += 1
        globals.state = NodeRole.Candidate
        # todo: how to vote for self?
        globals.voted_for = globals.name

        # TODO: Make this async
        # TODO: If appendRpc received from someone else with term >= this term: become follower
        votes_received, election_start = 0, time.time()
        peer_ips = transport.peer_ips
        for peer in peer_ips:
            if self.request_vote(peer):
                log_me(f"{globals.name} received vote from: {peer}")
                votes_received += 1

            # Split vote
            if time.time() - election_start > globals.election_timeout:
                log_me(f"{globals.name} observed a split vote")
                time.sleep(random_timeout(globals.LOW_TIMEOUT, globals.HIGH_TIMEOUT))
                # If someone else became leader in the meantime, exit out
                if globals.state != NodeRole.Candidate: return
                self.trigger_election()

        # Case 1: got majority votes: become leader
        if votes_received >= 1 + len(peer_ips) // 2:
            globals.state = NodeRole.Leader
            log_me(f"{globals.name} became leader!")
            self.init_heartbeat()

    def request_vote(self, peer):
        log_me(f'[Requesting vote from] {peer}')
        response = transport.request_vote(peer=peer)
        return response is not None and response.vote_granted


election = Election()
