import enum
import os
import time

from queue import Queue
from threading import Lock, Thread
from raft import config as config

# from .config import config 
from .log_manager import *
from .transport import *

class Election:
    def __init__(self, term, log_manager, transport: Transport):
        self.timeout_thread = None
        self.log_manager = log_manager
        self.__transport = transport
        self.__lock = Lock()
        self.init_timeout()

    def init_heartbeat(self):
        '''
        Initiate periodic heartbeats to the follower nodes if node is leader
        '''
        if self.log_manager.role != NodeRole.Leader:
            return
        print(f"Node is leader for the term {self.log_manager.current_term}")
        print('Starting periodic heartbeats to peers')
        # send heartbeat to peers once peers are added to transport
        for peer in self.log_manager.peers:
            Thread(target=self.send_heartbeat, args=(peer,)).start()

    def send_heartbeat(self, peer: str):
        '''
        SEND heartbeat to the peers and get it's response if LEADER
        '''
        try:
            while self.log_manager.role == NodeRole.Leader:
                print(f'[PEER HEARTBEAT] {peer}')
                start = time.time()
                response = self.__transport.send_heartbeat(peer=peer)
                if response:
                    # Peer has higher term. Relinquish leadership
                    if response.term > self.log_manager.current_term:
                        self.log_manager.current_term = response.term
                        self.log_manager.role = NodeRole.Follower
                        self.init_timeout()
                delta = time.time() - start
                time.sleep((config.HB_TIME - delta) / 1000)
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
        while self.log_manager.role != NodeRole.Leader:
            delta = self.log_manager.election_time() - time.time()
            if delta < 0:
                # TODO: START ELECTION!
                print("TODO")
            else:
                time.sleep(delta)

    def init_timeout(self):
        '''
        Checks for missed heartbeats from the leader and start the election
        '''
        try:
            print('Starting timeout')
            self.log_manager.reset_timeout()
            if self.timeout_thread and self.timeout_thread.is_alive():
                return
            self.timeout_thread = Thread(target=self.timeout_loop)
            self.timeout_thread.start()
        except Exception as e:
            raise e

