import time
from threading import Thread

from .config import NodeRole, globals
from .utils import *
from .transport import transport
from .log_manager import log_manager


class Election:

    def __init__(self):
        self.timeout_thread = None

    # TODO:Who triggers this?
    def init_heartbeat(self):
        """Initiate periodic heartbeats to the follower nodes if node is leader"""
        if log_manager.role != NodeRole.Leader: return

        log_me(f"Node is leader for the term {globals.current_term}, Starting periodic heartbeats to peers")
        # send heartbeat to peers once peers are added to transport
        for peer in transport.peers:
            Thread(target=self.send_heartbeat, args=(peer,)).start()

    def send_heartbeat(self, peer: str):
        """SEND heartbeat to the peers and get response if LEADER"""

        try:
            while globals.role == NodeRole.Leader:
                log_me(f'[PEER HEARTBEAT] {peer}')
                start = time.time()
                response = transport.send_heartbeat(peer=peer)
                if response:
                    # Peer has higher term. Relinquish leadership
                    if response.term > globals.current_term:
                        globals.current_term = response.term
                        globals.role = NodeRole.Follower
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
        while log_manager.role != NodeRole.Leader:
            # TODO: condition looks wrong. method isn't implemented in log_manager
            delta = log_manager.election_time() - time.time()
            if delta < 0:
                self.trigger_election()
                print("TODO")
            else:
                time.sleep(delta)

    def init_timeout(self):
        """Checks for missed heartbeats from the leader and start the election"""

        try:
            print('Starting timeout')
            log_manager.reset_timeout()
            if self.timeout_thread and self.timeout_thread.is_alive():
                return
            self.timeout_thread = Thread(target=self.timeout_loop)
            self.timeout_thread.start()
        except Exception as e:
            raise e

    def trigger_election(self):
        globals.current_term += 1
        election_start = time.time()
        # todo: how to vote for self?
        globals.voted_for = globals.name

        # TODO: Make this async
        # TODO: If appendRpc received from someone else with term >= this term: become follower
        votes_received = 0
        peer_ips = transport.peer_ips
        for peer in peer_ips:
            response = self.request_vote(peer)
            if response.vote_granted: votes_received += 1

            # Split vote
            if time.time() - election_start > globals.election_timeout:
                time.sleep(random_timeout(globals.LOW_TIMEOUT, globals.HIGH_TIMEOUT))
                self.trigger_election()

        # Case 1: got majority votes: become leader
        if votes_received >= 1 + len(peer_ips) // 2:
            globals.state = NodeRole.Leader

        # Case 2: got heartbeat inbetween: should be handled in heartbeat code

    def request_vote(self, peer):
        log_me(f'[Requesting vote from] {peer}')
        response = transport.request_vote(peer=peer)
        return response is not None and response.received_vote


election = Election()
